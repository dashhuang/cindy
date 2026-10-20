/**
 * Production upload/import round trip for the local-folder-path remote-chat bug.
 * Existing directory-download tests do not cover uploads or recipient isolation.
 * Minimal input: one file and an empty subdirectory, transferred in small parts.
 * Unit tier: temporary files only, fake transport; no network or child processes.
 */
import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { buildAttachmentOssRef, type MigrationFileRef } from '@cindy/device-link';
import { FolderImports, uploadChatFolder, type FolderUploadDeps } from '../chat-folder-transfer';

let root: string;
beforeEach(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), 'chat-folder-test-'));
});
afterEach(async () => {
  await fs.rm(root, { recursive: true, force: true });
});

async function fixture() {
  const source = path.join(root, 'source', '资料');
  await fs.mkdir(path.join(source, 'empty'), { recursive: true });
  await fs.writeFile(path.join(source, 'note.txt'), 'folder contents');
  await fs.writeFile(path.join(source, '.hidden'), 'hidden contents');
  const sent = new Map<string, Buffer>();
  let owner = 'owner-a';
  const imports = new FolderImports({
    root: () => path.join(root, 'receiver'),
    owner: () => owner,
    receive: async (_controller, part, dest) => {
      const bytes = sent.get(part.ref);
      if (!bytes) throw new Error('missing part');
      await fs.writeFile(dest, bytes);
    },
  });
  const deps: FolderUploadDeps = {
    tempRoot: () => root,
    maxPartBytes: 1024,
    pollMs: 0,
    assertCurrent: () => {},
    removeRemote: vi.fn(),
    sendPart: async (_target, file, progress) => {
      const bytes = await fs.readFile(file);
      const size = bytes.length,
        sha256 = createHash('sha256').update(bytes).digest('hex');
      const ref = buildAttachmentOssRef({ ossKey: `folder/${sent.size}`, size, sha256 });
      sent.set(ref, bytes);
      progress(size);
      return { ref, size, sha256 };
    },
    op: async <T>(_device: string, request: Record<string, unknown>) => {
      if (request.op === 'caps') return { folderImport: true } as T;
      if (request.op === 'importDirStart')
        return {
          ok: true,
          transferId: imports.start('controller', request.name, request.file),
        } as T;
      if (request.op === 'importDirCancel') {
        await imports.cancel('controller', request.transferId as string);
        return { ok: true } as T;
      }
      return { ok: true, ...imports.status('controller', request.transferId as string) } as T;
    },
  };
  return {
    source,
    sent,
    deps,
    imports,
    changeOwner: () => {
      owner = 'owner-b';
    },
  };
}

it('copies nested/empty/hidden entries and returns only the recipient path; leaves source intact', async () => {
  const { source, deps, sent } = await fixture();
  const progress = vi.fn();
  const dest = await uploadChatFolder(
    { deviceId: 'target', sourcePath: source, requestId: 'r' },
    deps,
    progress,
    new AbortController().signal,
  );
  expect(dest.startsWith(path.join(root, 'receiver') + path.sep)).toBe(true);
  expect(await fs.readFile(path.join(dest, 'note.txt'), 'utf8')).toBe('folder contents');
  expect(await fs.readFile(path.join(dest, '.hidden'), 'utf8')).toBe('hidden contents');
  expect((await fs.stat(path.join(dest, 'empty'))).isDirectory()).toBe(true);
  expect(await fs.readFile(path.join(source, 'note.txt'), 'utf8')).toBe('folder contents');
  expect(sent.size).toBeGreaterThan(1);
  expect(progress.mock.calls.some((call) => call[2] === 'upload' && call[0] === call[1])).toBe(
    true,
  );
  expect((await fs.readdir(root)).some((name) => name.startsWith('cindy-folder-upload-'))).toBe(
    false,
  );
});

it('old peers fail before reading file contents or uploading', async () => {
  const { source, deps, sent } = await fixture();
  deps.op = async <T>() => ({ ok: true }) as T;
  await expect(
    uploadChatFolder(
      { deviceId: 'old', sourcePath: source, requestId: 'r' },
      deps,
      vi.fn(),
      new AbortController().signal,
    ),
  ).rejects.toThrow('REMOTE_UNSUPPORTED');
  expect(sent.size).toBe(0);
});

it('does not archive its own scratch files when the chosen folder contains the temp root', async () => {
  const { source, deps } = await fixture();
  deps.tempRoot = () => source;
  const dest = await uploadChatFolder(
    { deviceId: 'target', sourcePath: source, requestId: 'r' },
    deps, vi.fn(), new AbortController().signal,
  );
  expect((await fs.readdir(dest)).sort()).toEqual(['.hidden', 'empty', 'note.txt']);
});

it('a corrupt archive fails without exposing a partial directory or leaving extracted files', async () => {
  const { source, deps, sent } = await fixture();
  const op = deps.op;
  deps.op = async <T>(device: string, request: Record<string, unknown>) => {
    if (request.op === 'importDirStart') {
      for (const [key, bytes] of sent) sent.set(key, Buffer.alloc(bytes.length, 1));
    }
    return op<T>(device, request);
  };
  await expect(
    uploadChatFolder(
      { deviceId: 'target', sourcePath: source, requestId: 'r' },
      deps,
      vi.fn(),
      new AbortController().signal,
    ),
  ).rejects.toThrow('TRANSFER_FAILED');
  expect(await fs.readdir(path.join(root, 'receiver'))).toEqual([]);
});

it('cancellation stops transfer and removes the local archive without returning a local path', async () => {
  const { source, deps } = await fixture();
  const abort = new AbortController();
  const send = deps.sendPart;
  deps.sendPart = async (...args) => {
    const result = await send(...args);
    abort.abort();
    return result;
  };
  await expect(
    uploadChatFolder(
      { deviceId: 'target', sourcePath: source, requestId: 'r' },
      deps,
      vi.fn(),
      abort.signal,
    ),
  ).rejects.toThrow();
  expect((await fs.readdir(root)).some((name) => name.startsWith('cindy-folder-upload-'))).toBe(
    false,
  );
});

it('rejects malformed descriptors and traversal names before writing anything', async () => {
  const { imports } = await fixture();
  const invalid: MigrationFileRef = { ref: 'invalid', size: 10, sha256: 'a'.repeat(64) };
  expect(() => imports.start('controller', '../escape', invalid)).toThrow('BAD_ARGS');
  expect(() => imports.start('controller', 'folder', invalid)).toThrow('BAD_ARGS');
  await expect(fs.stat(path.join(root, 'receiver'))).rejects.toThrow();
});

it('import status and cancellation are bound to the controller and current account', async () => {
  const { source, deps, imports, changeOwner } = await fixture();
  let id = '';
  const op = deps.op;
  deps.op = async <T>(device: string, request: Record<string, unknown>) => {
    const result = await op<T>(device, request);
    if (request.op === 'importDirStart') id = (result as { transferId: string }).transferId;
    return result;
  };
  const dest = await uploadChatFolder(
    { deviceId: 'target', sourcePath: source, requestId: 'r' },
    deps,
    vi.fn(),
    new AbortController().signal,
  );
  expect(() => imports.status('other-controller', id)).toThrow('BAD_ARGS');
  await expect(imports.cancel('other-controller', id)).rejects.toThrow('BAD_ARGS');
  expect((await fs.stat(dest)).isDirectory()).toBe(true);
  changeOwner();
  expect(() => imports.status('controller', id)).toThrow('BAD_ARGS');
});

it('late cancellation removes the completed copy but never the source folder', async () => {
  const { source, deps, imports } = await fixture();
  let id = '';
  const op = deps.op;
  deps.op = async <T>(device: string, request: Record<string, unknown>) => {
    const result = await op<T>(device, request);
    if (request.op === 'importDirStart') id = (result as { transferId: string }).transferId;
    return result;
  };
  const dest = await uploadChatFolder(
    { deviceId: 'target', sourcePath: source, requestId: 'r' },
    deps,
    vi.fn(),
    new AbortController().signal,
  );
  await imports.cancel('controller', id);
  await expect(fs.stat(dest)).rejects.toThrow();
  expect((await fs.stat(source)).isDirectory()).toBe(true);
});
