// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightCatppuccin from '@catppuccin/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Open Data Platform',
			description:
				'Reproducible open-source data platform: local Compose baseline and planned AWS path as Terraform IaC.',
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/FilipeOliveira94/open-data-platform',
				},
			],
			editLink: {
				baseUrl:
					'https://github.com/FilipeOliveira94/open-data-platform/edit/main/site/',
			},
			sidebar: [
				{ label: 'Home', slug: '' },
				{ label: 'Architecture', slug: 'architecture' },
				{ label: 'Pipeline', slug: 'pipeline' },
				{ label: 'Operations', slug: 'operations' },
				{ label: 'Reference', slug: 'reference' },
			],
			plugins: [
				starlightCatppuccin({
					// Preview: https://starlight-themes.netlify.app/catppuccin/
					dark: { flavor: 'mocha', accent: 'teal' },
					light: { flavor: 'latte', accent: 'teal' },
				}),
			],
		}),
	],
});
