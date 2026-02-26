const fs = require('fs');
const path = require('path');
const AdmZip = require('adm-zip');

const appDir = path.join(__dirname, 'ppfg-app');
const pkg = JSON.parse(fs.readFileSync(path.join(appDir, 'package.json'), 'utf8'));
const version = pkg.version;
const randStr = Math.random().toString(36).substring(2, 10);
const zipName = `corva_ppfg_template_importer_ui-${version}-${randStr}.zip`;
const zipPath = path.join(appDir, zipName);

const filesToInclude = [
  'package.json', 'yarn.lock', 'config-overrides.js', 'manifest.json',
  '.eslintrc', '.prettierrc',
  'src/index.js', 'src/App.js', 'src/App.css', 'src/styles.css',
  'src/ppfgHtml.js', 'src/AppSettings.js', 'src/constants.js',
  'src/components/index.js', 'src/components/WITSSummaryChart/index.js',
  'src/components/WITSSummaryChart/options.js', 'src/components/WITSSummaryChart/styles.css',
  'src/effects/index.js', 'src/effects/useWITSSummaryData.js',
  'src/assets/logo.svg',
  'config/jest/babelTransform.js', 'config/jest/cssTransform.js',
  'config/jest/fileTransform.js', 'config/jest/globalSetup.js', 'config/jest/setupTests.js'
];

const zip = new AdmZip();
let count = 0;

for (const f of filesToInclude) {
  const src = path.join(appDir, f);
  if (fs.existsSync(src)) {
    const dir = path.dirname(f).replace(/\\/g, '/');
    zip.addLocalFile(src, dir === '.' ? '' : dir);
    count++;
  } else {
    console.log('MISSING:', f);
  }
}

zip.writeZip(zipPath);
console.log(`Zipped ${count} files -> ${zipName} (${(fs.statSync(zipPath).size / 1024).toFixed(1)} KB)`);

// Verify entries use forward slashes
const verify = new AdmZip(zipPath);
const entries = verify.getEntries();
console.log('\nZip entries:');
entries.forEach(e => console.log('  ' + e.entryName));

const hasIndex = entries.some(e => e.entryName === 'src/index.js');
console.log(hasIndex ? '\nVERIFIED: src/index.js found with forward slashes' : '\nERROR: src/index.js not found!');
