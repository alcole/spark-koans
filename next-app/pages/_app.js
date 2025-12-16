/**
 * Next.js App Wrapper
 * This wraps all pages
 */

import '../styles/globals.css';

export default function App({ Component, pageProps }) {
  return <Component {...pageProps} />;
}
