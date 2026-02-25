import { Checkbox, FormControlLabel } from '@material-ui/core';
import PropTypes from 'prop-types';

import { DATASETS } from './constants';

function AppSettings({
  settings: apiSettings,
  onSettingChange,
  // appData,
  // app,
  // user,
  // company,
}) {
  const settings = { ...DATASETS, ...apiSettings };
  return (
    <div>
      <FormControlLabel
        label="Example checkbox"
        control={
          <Checkbox
            checked={settings.isExampleCheckboxChecked}
            onChange={e => onSettingChange('isExampleCheckboxChecked', e.target.checked)}
          />
        }
      />
    </div>
  );
}

AppSettings.propTypes = {
  app: PropTypes.shape({}).isRequired,
  appData: PropTypes.shape({}).isRequired,
  company: PropTypes.shape({}),
  onSettingChange: PropTypes.func.isRequired,
  settings: PropTypes.shape({
    isExampleCheckboxChecked: PropTypes.bool,
  }).isRequired,
  user: PropTypes.shape({}),
};

AppSettings.defaultProps = {
  user: {},
  company: {},
};

// Important: Do not change root component default export (AppSettings.js). Use it as container
//  for your App Settings. It's required to make build and zip scripts work as expected;
export default AppSettings;
