const { spawn } = require('child_process');

const child = spawn('npx', ['create-corva-app', 'zip', '.'], {
  cwd: __dirname + '/ppfg-app',
  shell: true,
  stdio: ['pipe', 'pipe', 'pipe']
});

let output = '';
child.stdout.on('data', d => {
  output += d.toString();
  process.stdout.write(d);
  if (output.includes('Skip version bump')) {
    setTimeout(() => {
      child.stdin.write('\x1B[B');
      setTimeout(() => {
        child.stdin.write('\x1B[B');
        setTimeout(() => {
          child.stdin.write('\r');
        }, 300);
      }, 300);
    }, 300);
  }
});
child.stderr.on('data', d => process.stderr.write(d));
child.on('close', code => {
  console.log('\nExit code:', code);
  process.exit(code);
});
