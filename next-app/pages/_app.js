/**
 * Next.js App Wrapper
 * This wraps all pages
 */

import '../styles/globals.css';
import { Analytics } from '@vercel/analytics/react';

export default function App({ Component, pageProps }) {
  return (
    <>
      <Component {...pageProps} />
      <Analytics />
    </>
  );
}
