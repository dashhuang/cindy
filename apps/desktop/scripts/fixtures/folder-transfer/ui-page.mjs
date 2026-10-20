export default {
  name: 'folder-transfer',
  entry: './folder-transfer-fixture.tsx',
  viewport: { width: 520, height: 320 },
  real: 'ComposerFolderTransfers, production progress card, translations, themes and spinner',
  notCovered:
    'Actual device transfer and chat sending; covered by chatFolderTransfer and ChatInput unit tests. Windows runtime is not exercised.',
  cases: {
    default: { about: 'Uploading folder and failed folder with cancellation' },
    narrow: { about: 'Long folder name at narrow width', viewport: { width: 320, height: 360 } },
  },
};
