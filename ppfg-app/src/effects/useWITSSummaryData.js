import { useState, useEffect } from 'react';
import { corvaDataAPI, socketClient } from '@corva/ui/clients';
import { reverse } from 'lodash';

async function fetchWITSSummaryData({ assetId, dataset }) {
  try {
    return await corvaDataAPI.get(`/api/v1/data/corva/${dataset}/`, {
      limit: 1000,
      skip: 0, // NOTE: Required for pagination
      // NOTE: Make sure the sort field hit database indexes. Otherwise the request will take too long
      sort: JSON.stringify({ timestamp: -1 }),
      query: JSON.stringify({ asset_id: assetId }),
      // NOTE: To make efficient request - fetch only fields used by the app
      fields: ['timestamp', 'data.hole_depth', 'data.state'].join(','),
    });
  } catch (e) {
    console.log(e);
    return [];
  }
}

function useWITSSummaryData({ assetId, dataset }) {
  // NOTE: Define state to store subscription data
  const [witsSummaryData, setWITSSummaryData] = useState([]);

  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let unsubscribe;
    setLoading(true);

    // Make initial request first
    fetchWITSSummaryData({ assetId, dataset })
      .then(response => {
        setWITSSummaryData(reverse(response));

        const subscription = { provider: 'corva', dataset, assetId }; // Subscription params
        // Concatenate new records to state
        const onDataReceive = event => setWITSSummaryData(prevData => prevData.concat(event.data));

        // NOTE: Subscribe to updates after initial data request
        unsubscribe = socketClient.subscribe(subscription, { onDataReceive });
      })
      .finally(() => setLoading(false));

    // NOTE: Unsubscribe to prevent memory leaks and crashes in your app.
    return () => unsubscribe?.();
  }, [assetId, dataset]);

  return { loading, witsSummaryData };
}

export default useWITSSummaryData;
