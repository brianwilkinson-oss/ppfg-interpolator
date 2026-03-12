import { useRef, useEffect } from 'react';
import { AppHeader } from '@corva/ui/components';
import { corvaDataAPI } from '@corva/ui/clients';
import htmlContent from './ppfgHtml';

function App(props) {
  const { appHeaderProps, well } = props;
  const iframeRef = useRef(null);
  const wellAssetId = well && well.asset_id;

  useEffect(() => {
    const iframe = iframeRef.current;
    if (!iframe) return;

    const doc = iframe.contentDocument;
    doc.open();
    doc.write(htmlContent);
    doc.close();

    const iframeWin = iframe.contentWindow;
    if (corvaDataAPI) iframeWin.corvaDataAPI = corvaDataAPI;

    if (wellAssetId) {
      const field = iframeWin.document.getElementById('corvaAssetId');
      if (field) field.value = wellAssetId;
    }
  }, [wellAssetId]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <AppHeader {...appHeaderProps} />
      <iframe
        ref={iframeRef}
        title="PPFG Interpolator"
        style={{
          flex: 1,
          width: '100%',
          border: 'none',
          background: '#1c1c1e',
        }}
        sandbox="allow-scripts allow-same-origin allow-downloads"
      />
    </div>
  );
}

export default App;
