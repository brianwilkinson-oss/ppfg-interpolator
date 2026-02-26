const https = require('https');
const fs = require('fs');
const path = require('path');

const zipPath = path.resolve(__dirname, 'corva_ppfg_template_importer_ui-0.0.9-ibl9krhn.zip');
const token = process.argv[2];
const appKey = 'corva.ppfg_template_importer.ui';

const boundary = '----FormBoundary' + Math.random().toString(36).substring(2);
const zipData = fs.readFileSync(zipPath);

const prefix = Buffer.from(
  '--' + boundary + '\r\n' +
  'Content-Disposition: form-data; name="package"; filename="package.zip"\r\n' +
  'Content-Type: application/zip\r\n\r\n'
);
const suffix = Buffer.from(
  '\r\n--' + boundary + '\r\n' +
  'Content-Disposition: form-data; name="github_username"\r\n\r\n' +
  'brianwilkinson-oss\r\n' +
  '--' + boundary + '--\r\n'
);

const body = Buffer.concat([prefix, zipData, suffix]);

const opts = {
  hostname: 'api.corva.ai',
  path: '/v2/apps/' + appKey + '/packages/upload',
  method: 'POST',
  headers: {
    'Content-Type': 'multipart/form-data; boundary=' + boundary,
    'Content-Length': body.length,
    'Authorization': 'Bearer ' + token,
    'Accept': 'application/json'
  }
};

const req = https.request(opts, res => {
  let d = '';
  res.on('data', c => d += c);
  res.on('end', () => {
    console.log('Status:', res.statusCode);
    console.log('Response:', d.substring(0, 2000));
  });
});
req.on('error', e => console.error('Error:', e.message));
req.write(body);
req.end();

