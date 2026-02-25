/* eslint-disable */

// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
// eslint-disable-next-line
import '@testing-library/jest-dom/extend-expect';

// Set UTC timezone for tests to not use the environment timezone
process.env.TZ = 'UTC';

// Mock ResizeObserver & MutationObserver
class FakeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

global.ResizeObserver = FakeObserver;
global.MutationObserver = FakeObserver;

// Suppressing "Could not parse CSS stylesheet" from JSDOM
const originalConsoleError = global.console.error;
global.console.error = (message, ...optionalParams) => {
  if (message.includes('Could not parse CSS stylesheet')) {
    return;
  }
  originalConsoleError(message, ...optionalParams);
};
