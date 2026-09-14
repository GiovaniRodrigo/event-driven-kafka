import React from 'react';
import ReactDOM from 'react-dom/client';
import './styles/globals.css';
import { ThemeProvider } from './components/theme-provider';
import { I18nProvider } from './i18n';
import { ToastProvider } from './components/ui';
import { App } from './App';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <I18nProvider>
      <ThemeProvider>
        <ToastProvider>
          <App />
        </ToastProvider>
      </ThemeProvider>
    </I18nProvider>
  </React.StrictMode>,
);
