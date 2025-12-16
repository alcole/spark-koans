#!/usr/bin/env node
/**
 * Script to extract koans from monolithic JSX files
 * and create individual koan files in the modular structure
 */

const fs = require('fs');
const path = require('path');

// Read the expanded koans file
const expandedFile = fs.readFileSync(
  path.join(__dirname, '../../pyspark-koans-expanded.jsx'),
  'utf8'
);

// Extract the KOANS array
const koansMatch = expandedFile.match(/const KOANS = \[([\s\S]*?)\n\];/);
if (!koansMatch) {
  console.error('Could not find KOANS array');
  process.exit(1);
}

// Parse koans - split by objects
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

console.log(`Found ${koans.length} koans to extract`);

// Process each koan
koans.forEach((koanText, index) => {
  try {
    // Extract key fields using regex
    const idMatch = koanText.match(/id:\s*(\d+)/);
    const titleMatch = koanText.match(/title:\s*"([^"]+)"/);
    const categoryMatch = koanText.match(/category:\s*"([^"]+)"/);

    if (!idMatch || !titleMatch) {
      console.warn(`Skipping koan ${index + 1}: missing id or title`);
      return;
    }

    const id = parseInt(idMatch[1]);
    const title = titleMatch[1];
    const category = categoryMatch ? categoryMatch[1] : 'Misc';

    // Skip if we already have this koan (1-5)
    if (id <= 5) {
      console.log(`Skipping koan ${id} (already exists)`);
      return;
    }

    // Determine folder based on category
    let folder;
    if (category === 'Basics') {
      folder = 'pyspark/basics';
    } else if (category === 'Column Operations') {
      folder = 'pyspark/column-ops';
    } else if (category === 'String Functions') {
      folder = 'pyspark/strings';
    } else if (category === 'Aggregations') {
      folder = 'pyspark/aggregations';
    } else if (category === 'Joins') {
      folder = 'pyspark/joins';
    } else if (category === 'Window Functions') {
      folder = 'pyspark/windows';
    } else if (category === 'Null Handling') {
      folder = 'pyspark/nulls';
    } else if (category === 'Advanced') {
      folder = 'pyspark/advanced';
    } else {
      folder = 'pyspark/misc';
    }

    // Create folder if it doesn't exist
    const folderPath = path.join(__dirname, '../src/koans', folder);
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
 * Category: ${category}
 */

export default ${koanText};
`;

    // Write file
    fs.writeFileSync(filePath, fileContent);
    console.log(`✓ Created ${folder}/${filename}`);

  } catch (error) {
    console.error(`Error processing koan ${index + 1}:`, error.message);
  }
});

console.log('\n✓ PySpark koans extraction complete!');
