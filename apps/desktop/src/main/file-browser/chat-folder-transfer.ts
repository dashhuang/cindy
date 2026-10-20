import { randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream, promises as fs } from 'node:fs';
import path from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';
import {
  parseAttachmentOssRef,
  parsePeerAttachmentRef,
  type MigrationFile,
  type MigrationFileRef,
} from '@cindy/device-link';
import { packDirectory, extractDirectoryArchive } from './dir-archive';
import { sendParts, receiveParts } from '../task-migration/transferParts';
import type { DirExportDeps } from './dir-export';
import { sanitizeSaveFileName } from '../cindy-brain/dirDeposit';
import { assertDiskCapacity } from '../task-migration/resources';
import { buildSystemPathBlocklist, isPathAllowedAgainst } from '../filePathPolicy';

export interface ChatFolderUploadArgs {
  deviceId: string;
  sourcePath: string;
  requestId: string;
}

export type FolderImportStatus =
  { state: 'receiving' | 'extracting' } | { state: 'done'; path: string } | { state: 'error' };

function partsOf(file: MigrationFile): MigrationFileRef[] {
  return 'parts' in file ? file.parts : [file];
}

/** Validate the complete descriptor before creating directories or downloading bytes. */
function parseFile(raw: unknown): MigrationFile {
  const file = raw as MigrationFile;
  if (!file || !Number.isSafeInteger(file.size) || file.size < 0) throw new Error('BAD_ARGS');
  const parts = partsOf(file);
  if (!Array.isArray(parts) || !parts.length || parts.length > 10_000) throw new Error('BAD_ARGS');
  let total = 0;
  for (const part of parts) {
    const ref =
      part && typeof part.ref === 'string' && part.ref.length <= 4096
        ? (parsePeerAttachmentRef(part.ref) ?? parseAttachmentOssRef(part.ref))
        : null;
    if (
      !ref ||
      !Number.isSafeInteger(part.size) ||
      part.size < 0 ||
      !/^[a-f0-9]{64}$/.test(part.sha256) ||
      ref.size !== part.size ||
      ref.sha256 !== part.sha256
    )
      throw new Error('BAD_ARGS');
    total += part.size;
  }
  if (!Number.isSafeInteger(total) || total !== file.size) throw new Error('BAD_ARGS');
  return file;
}

interface ImportJob {
  controller: string;
  owner: string;
  polled: number;
  abort: AbortController;
  status: FolderImportStatus;
  directory?: string;
}

export interface FolderImportDeps {
  owner(): string;
  root(): string;
  receive(
    controller: string,
    part: MigrationFileRef,
    dest: string,
    signal: AbortSignal,
  ): Promise<void>;
}

/** Same start/status pattern as directory downloads: the relay never waits for disk I/O. */
export class FolderImports {
  private readonly jobs = new Map<string, ImportJob>();
  constructor(private readonly deps: FolderImportDeps) {}

  start(controller: string, name: unknown, rawFile: unknown): string {
    if (
      typeof name !== 'string' ||
      !name ||
      name.length > 255 ||
      /[/\\\0]/.test(name) ||
      name === '.' ||
      name === '..'
    )
      throw new Error('BAD_ARGS');
    const file = parseFile(rawFile);
    const id = randomUUID();
    const job: ImportJob = {
      controller,
      owner: this.deps.owner(),
      polled: Date.now(),
      abort: new AbortController(),
      status: { state: 'receiving' },
    };
    const root = this.deps.root();
    this.jobs.set(id, job);
    const watchdog = setInterval(() => {
      if (Date.now() - job.polled > 120_000 || job.owner !== this.deps.owner()) job.abort.abort();
    }, 30_000);
    watchdog.unref?.();
    void this.receive(job, root, sanitizeSaveFileName(name), file).finally(() => {
      clearInterval(watchdog);
      const expiry = setTimeout(() => this.jobs.delete(id), 10 * 60_000);
      expiry.unref?.();
    });
    return id;
  }

  status(controller: string, id: string): FolderImportStatus {
    const job = this.get(controller, id);
    job.polled = Date.now();
    return job.status;
  }

  async cancel(controller: string, id: string): Promise<void> {
    const job = this.get(controller, id);
    job.abort.abort();
    // A late cancellation can arrive after extraction completed.
    if (job.status.state === 'done' && job.directory) {
      await fs.rm(job.directory, { recursive: true, force: true });
      job.status = { state: 'error' };
    }
  }

  private get(controller: string, id: string): ImportJob {
    const job = this.jobs.get(id);
    if (!job || job.controller !== controller || job.owner !== this.deps.owner())
      throw new Error('BAD_ARGS');
    return job;
  }

  private async receive(job: ImportJob, root: string, name: string, file: MigrationFile) {
    const signal = job.abort.signal;
    const check = () => {
      signal.throwIfAborted();
      if (job.owner !== this.deps.owner()) throw new Error('CANCELLED');
    };
    try {
      check();
      await fs.mkdir(root, { recursive: true });
      job.directory = await fs.mkdtemp(path.join(root, 'folder-'));
      await assertDiskCapacity([{ path: job.directory, bytes: file.size * 2 }]);
      const archive = path.join(job.directory, 'archive.tar');
      await receiveParts(file, archive, async (part, dest) => {
        check();
        await this.deps.receive(job.controller, part, dest, signal);
        check();
      });
      check();
      job.status = { state: 'extracting' };
      const dest = path.join(job.directory, 'contents', name);
      await fs.mkdir(dest, { recursive: true });
      const skipped = await extractDirectoryArchive(createReadStream(archive), dest, signal);
      if (skipped) throw new Error('INCOMPLETE_FOLDER');
      check();
      await fs.unlink(archive);
      check();
      job.status = { state: 'done', path: dest };
    } catch {
      if (job.directory)
        await fs.rm(job.directory, { recursive: true, force: true }).catch(() => {});
      job.status = { state: 'error' };
    }
  }
}

export interface FolderUploadDeps extends DirExportDeps {
  op<T>(deviceId: string, args: Record<string, unknown>): Promise<T>;
  assertCurrent(): void;
  pollMs?: number;
}

/** A folder is copied once. Only the receiving computer's path leaves this function. */
export async function uploadChatFolder(
  args: ChatFolderUploadArgs,
  deps: FolderUploadDeps,
  progress: (received: number, total: number, phase: 'pack' | 'upload' | 'extract') => void,
  signal: AbortSignal,
): Promise<string> {
  const check = () => {
    signal.throwIfAborted();
    deps.assertCurrent();
  };
  check();
  if (
    !args ||
    typeof args.deviceId !== 'string' ||
    !args.deviceId ||
    typeof args.sourcePath !== 'string' ||
    !path.isAbsolute(args.sourcePath)
  )
    throw new Error('BAD_ARGS');
  const source = await fs.realpath(args.sourcePath);
  if (
    !isPathAllowedAgainst(source, buildSystemPathBlocklist()) ||
    !(await fs.stat(source)).isDirectory()
  )
    throw new Error('BAD_ARGS');
  const op = <T>(request: Record<string, unknown>) => {
    check();
    return deps.op<T>(args.deviceId, { workdir: '.', ...request });
  };
  const caps = await op<{ folderImport?: boolean }>({ op: 'caps' });
  if (caps?.folderImport !== true) throw new Error('REMOTE_UNSUPPORTED');
  check();
  const temp = await fs.mkdtemp(path.join(await fs.realpath(deps.tempRoot()), 'cindy-folder-upload-'));
  const uploaded: string[] = [];
  let importId: string | undefined;
  let accepted = false;
  try {
    const archive = path.join(temp, 'archive.tar');
    let packed = 0,
      skipped = 0;
    progress(0, 0, 'pack');
    await pipeline(
      packDirectory(source, () => skipped++, temp),
      new Transform({
        transform(chunk: Buffer, _encoding, done) {
          try {
            check();
            packed += chunk.length;
            progress(packed, 0, 'pack');
            done(null, chunk);
          } catch (error) {
            done(error as Error);
          }
        },
      }),
      createWriteStream(archive, { flags: 'wx', mode: 0o600 }),
      { signal },
    );
    if (skipped) throw new Error('INCOMPLETE_FOLDER');
    check();
    let sent = 0;
    const file = await sendParts(
      archive,
      deps.maxPartBytes,
      async (part) => {
        check();
        const ref = await deps.sendPart(
          args.deviceId,
          part,
          (bytes) => progress(sent + bytes, packed, 'upload'),
          signal,
        );
        if (ref.ossKey) uploaded.push(ref.ossKey);
        sent += ref.size;
        return ref;
      },
      signal,
    );
    check();
    const start = await op<{ ok: boolean; transferId?: string }>({
      op: 'importDirStart',
      name: path.basename(source),
      file,
    });
    importId = start.transferId;
    if (!start.ok || !importId) throw new Error('TRANSFER_FAILED');
    progress(packed, packed, 'extract');
    while (true) {
      check();
      const status = await op<FolderImportStatus & { ok: boolean }>({
        op: 'importDirStatus',
        transferId: importId,
      });
      check();
      if (!status.ok || status.state === 'error') throw new Error('TRANSFER_FAILED');
      if (status.state === 'done') {
        if (typeof status.path !== 'string' || !status.path) throw new Error('TRANSFER_FAILED');
        accepted = true;
        return status.path;
      }
      await delay(deps.pollMs ?? 750, undefined, { signal });
    }
  } finally {
    if (importId && !accepted) {
      // Cancellation is scoped to this transfer, never the shared connection.
      try {
        deps.assertCurrent();
        await deps.op(args.deviceId, {
          workdir: '.',
          op: 'importDirCancel',
          transferId: importId,
        });
      } catch {
        /* The receiver also expires abandoned imports. */
      }
    }
    for (const key of uploaded) deps.removeRemote(key);
    await fs.rm(temp, { recursive: true, force: true });
  }
}
