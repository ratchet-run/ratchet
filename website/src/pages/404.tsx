import {useEffect, type ReactNode} from 'react';
import NotFound from '@theme/NotFound';

export default function Custom404(): ReactNode {
  useEffect(() => {
    window.umami?.track('not-found', {
      path: window.location.pathname,
      referrer: document.referrer || 'direct',
    });
  }, []);

  return <NotFound />;
}
