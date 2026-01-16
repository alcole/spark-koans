#!/usr/bin/env node

/**
 * Script to fix anonymous default export warnings in koan files
 * Changes `export default { ... }` to `const koan = { ... }; export default koan;`
 */

const fs = require('fs');
const path = require('path');
const glob = require('glob');

function fixKoanFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');

  // Skip if already fixed
  if (content.includes('const koan = {')) {
    console.log(`Skipped (already fixed): ${filePath}`);
    return;
  }

  // Skip index.js
  if (filePath.endsWith('index.js')) {
    console.log(`Skipped (index file): ${filePath}`);
    return;
  }

  // Match export default { ... } pattern
  const exportPattern = /^export default \{$/m;

  if (!exportPattern.test(content)) {
    console.log(`Skipped (no match): ${filePath}`);
    return;
  }

  // Replace export default { with const koan = {
  const newContent = content.replace(/^export default \{$/m, 'const koan = {')
    .replace(/\};(\s*)$/, '};\n\nexport default koan;\n');

  fs.writeFileSync(filePath, newContent, 'utf8');
  console.log(`Fixed: ${filePath}`);
}

// Find all koan files
const koanFiles = glob.sync('src/koans/**/*.js', {
  cwd: __dirname + '/..',
  absolute: true,
  ignore: ['**/index.js']
});

console.log(`Found ${koanFiles.length} koan files\n`);

koanFiles.forEach(fixKoanFile);

console.log('\n✓ Done!');
