// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Open Data Platform',
			description:
				'Reproducible open-source data platform on a local Docker Compose stack.',
			logo: {
				light: './src/assets/brand/fi-mark-light.svg',
				dark: './src/assets/brand/fi-mark-dark.svg',
			},
			customCss: ['./src/styles/brand.css'],
			components: {
				SocialIcons: './src/components/SocialIcons.astro',
			},
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
		}),
	],
});
