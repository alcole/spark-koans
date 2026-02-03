import { getAllKoanIds } from '../src/koans';

const BASE_URL = 'https://spark-koans.com';

const staticPages = [
  { path: '',       priority: '1.0' },
  { path: '/about', priority: '0.7' },
  { path: '/docs',  priority: '0.7' },
  { path: '/badge', priority: '0.5' },
];

export async function getServerSideProps({ res }) {
  const koanIds = getAllKoanIds();

  const urls = [
    ...staticPages.map(({ path, priority }) =>
      `  <url><loc>${BASE_URL}${path}</loc><changefreq>monthly</changefreq><priority>${priority}</priority></url>`
    ),
    ...koanIds.map(id =>
      `  <url><loc>${BASE_URL}/koans/${id}</loc><changefreq>monthly</changefreq><priority>0.9</priority></url>`
    ),
  ];

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...urls,
    '</urlset>',
  ].join('\n');

  res.setHeader('Content-Type', 'application/xml');
  res.write(xml);
  res.end();

  return { props: {} };
}

export default function Sitemap() {
  return null;
}
