import type { StorybookConfig } from '@storybook/react-vite';

const config: StorybookConfig = {
  stories: ['../src/**/*.stories.@(ts|tsx)'],
  addons: ['@storybook/addon-essentials', '@storybook/addon-a11y', '@storybook/addon-themes'],
  framework: { name: '@storybook/react-vite', options: {} },
  core: { disableTelemetry: true },
  // Relative base so the static build works under a GitHub Pages project subpath.
  async viteFinal(config) {
    return { ...config, base: './' };
  },
};

export default config;
