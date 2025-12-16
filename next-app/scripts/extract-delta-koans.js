#!/usr/bin/env node
/**
 * Script to extract Delta Lake koans from complete file
 */

const fs = require('fs');
const path = require('path');

// Read the delta complete file
const deltaFile = fs.readFileSync(
  path.join(__dirname, '../../pyspark-delta-koans-complete.jsx'),
  'utf8'
);

// Extract the KOANS array
const koansMatch = deltaFile.match(/const KOANS = \[([\s\S]*?)\n\];/);
if (!koansMatch) {
  console.error('Could not find KOANS array');
  process.exit(1);
}

const koansText = koansMatch[1];

// Split into individual koan objects
let koans = [];
let currentKoan = '';
let braceCount = 0;
let inKoan = false;

for (let i = 0; i < koansText.length; i++) {
  const char = koansText[i];

  if (char === '{' && !inKoan) {
    inKoan = true;
    braceCount = 1;
    currentKoan = '{';
  } else if (inKoan) {
    currentKoan += char;
    if (char === '{') braceCount++;
    if (char === '}') braceCount--;

    if (braceCount === 0) {
      koans.push(currentKoan);
      currentKoan = '';
      inKoan = false;
    }
  }
}

console.log(`Found ${koans.length} total koans`);

// Filter for Delta Lake koans only (id >= 101)
let deltaKoans = [];
koans.forEach((koanText) => {
  const idMatch = koanText.match(/id:\s*(\d+)/);
  if (idMatch && parseInt(idMatch[1]) >= 101) {
    deltaKoans.push(koanText);
  }
});

console.log(`Found ${deltaKoans.length} Delta Lake koans to extract`);

// Process each Delta Lake koan
deltaKoans.forEach((koanText, index) => {
  try {
    const idMatch = koanText.match(/id:\s*(\d+)/);
    const titleMatch = koanText.match(/title:\s*"([^"]+)"/);

    if (!idMatch || !titleMatch) {
      console.warn(`Skipping koan ${index + 1}: missing id or title`);
      return;
    }

    const id = parseInt(idMatch[1]);
    const title = titleMatch[1];

    // Skip if we already have this koan (101)
    if (id === 101) {
      console.log(`Skipping koan ${id} (already exists)`);
      return;
    }

    // Create folder if it doesn't exist
    const folderPath = path.join(__dirname, '../src/koans/delta');
    fs.mkdirSync(folderPath, { recursive: true });

    // Generate filename
    const fileId = String(id).padStart(3, '0');
    const titleSlug = title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '');
    const filename = `koan-${fileId}-${titleSlug}.js`;
    const filePath = path.join(folderPath, filename);

    // Create file content
    const fileContent = `/**
 * Koan ${id}: ${title}
 * Category: Delta Lake
 */

export default ${koanText};
`;

    // Write file
    fs.writeFileSync(filePath, fileContent);
    console.log(`✓ Created delta/${filename}`);

  } catch (error) {
    console.error(`Error processing koan ${index + 1}:`, error.message);
  }
});

console.log('\n✓ Delta Lake koans extraction complete!');
