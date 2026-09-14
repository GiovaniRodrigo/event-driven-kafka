import type { Preview } from '@storybook/react';
import { withThemeByClassName } from '@storybook/addon-themes';
import '../src/styles/globals.css';

/**
 * Storybook renders themed: the toolbar switch toggles the `dark`/`light` class
 * on <html>, driving the same token palette the app uses (dark-first). Every
 * story sits on the design-system background.
 */
const preview: Preview = {
  parameters: {
    controls: { matchers: { color: /(background|color)$/i, date: /Date$/i } },
    backgrounds: { disable: true },
    layout: 'padded',
  },
  decorators: [
    withThemeByClassName({
      themes: { dark: 'dark', light: 'light' },
      defaultTheme: 'dark',
      parentSelector: 'html',
    }),
  ],
};

export default preview;
