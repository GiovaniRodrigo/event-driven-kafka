import React from 'react';
import ReactDOM from 'react-dom/client';
import './styles/globals.css';
import { ThemeProvider } from './components/theme-provider';
import { ToastProvider } from './components/ui';
import { App } from './App';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeProvider>
      <ToastProvider>
        <App />
      </ToastProvider>
    </ThemeProvider>
  </React.StrictMode>,
);
