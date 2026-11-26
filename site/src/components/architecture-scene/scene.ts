/**
 * Dependency-free 3D renderer for the home-page architecture map.
 * A perspective camera orbits the scene; edges are quadratic curves in
 * world space with particles travelling along them.
 */
import { EDGES, GROUPS, MOBILE_POS, NODES, type FlowKind, type GroupId, type SceneEdge, type SceneNode, type Vec3 } from './data';

// Brand palette, dark tokens (filipeoliveira-hub-assets/brand/tokens.css).
const C = {
	bg: '#120B20',
	c0: '#1A1229',
	b1: '#2B1E4A',
	b2: '#42306F',
	l2: '#8A62E8',
	l3: '#B08CFF',
	l4: '#DECDFF',
	ink: '#F1ECFA',
	muted: '#B3A8C9',
	dot: '#FF6FA6',
};

const FONT = "Urbanist, 'Segoe UI', system-ui, sans-serif";

interface FlowStyle {
	width: number;
	dash: number[];
	speed: number;
	count: number;
	lift: number;
	alpha: number;
}

const FLOW: Record<FlowKind, FlowStyle> = {
	batch: { width: 1.5, dash: [], speed: 0.14, count: 2, lift: 1.0, alpha: 0.6 },
	stream: { width: 2.6, dash: [], speed: 0.38, count: 6, lift: 0.6, alpha: 0.7 },
	control: { width: 1, dash: [5, 6], speed: 0.09, count: 1, lift: 0, alpha: 0.5 },
	telemetry: { width: 1, dash: [2, 5], speed: 0.1, count: 1, lift: 0.8, alpha: 0.4 },
};

const LAYERS = [
	{ name: 'Bronze', y: 0.2, r: 1.6, color: C.l2 },
	{ name: 'Silver', y: 0.95, r: 1.35, color: C.l3 },
	{ name: 'Gold', y: 1.7, r: 1.1, color: C.dot },
];

const rgbCache = new Map<string, [number, number, number]>();
function rgba(hex: string, a: number): string {
	let rgb = rgbCache.get(hex);
	if (!rgb) {
		const n = Number.parseInt(hex.slice(1), 16);
		rgb = [(n >> 16) & 255, (n >> 8) & 255, n & 255];
		rgbCache.set(hex, rgb);
	}
	return `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${Math.max(0, Math.min(1, a))})`;
}

interface Proj {
	x: number;
	y: number;
	depth: number;
	s: number;
}

interface EdgeGeom {
	edge: SceneEdge;
	p0: Vec3;
	p1: Vec3;
	p2: Vec3;
	phase: number;
}

function bezier(e: EdgeGeom, t: number): Vec3 {
	const u = 1 - t;
	const a = u * u;
	const b = 2 * u * t;
	const c = t * t;
	return [
		a * e.p0[0] + b * e.p1[0] + c * e.p2[0],
		a * e.p0[1] + b * e.p1[1] + c * e.p2[1],
		a * e.p0[2] + b * e.p1[2] + c * e.p2[2],
	];
}

function makeGlow(color: string): HTMLCanvasElement {
	const g = document.createElement('canvas');
	g.width = g.height = 64;
	const x = g.getContext('2d')!;
	const grad = x.createRadialGradient(32, 32, 0, 32, 32, 32);
	grad.addColorStop(0, rgba(color, 0.9));
	grad.addColorStop(0.25, rgba(color, 0.35));
	grad.addColorStop(1, rgba(color, 0));
	x.fillStyle = grad;
	x.fillRect(0, 0, 64, 64);
	return g;
}

export function mountScene(root: HTMLElement): void {
	const canvas = root.querySelector('canvas');
	const card = root.querySelector<HTMLElement>('[data-card]');
	const ctx = canvas?.getContext('2d');
	if (!canvas || !card || !ctx) return;

	const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
	const byId = new Map(NODES.map((n) => [n.id, n]));
	const groupLabel = new Map(GROUPS.map((g) => [g.id, g.label]));
	const glow = { pink: makeGlow(C.dot), ink: makeGlow(C.l4) };
	// Each function (group) has its own hue; nodes, flows and particles take it.
	const roleColor = new Map(GROUPS.map((g) => [g.id, g.color]));
	const roleGlow = new Map(GROUPS.map((g) => [g.id, makeGlow(g.color)]));
	const colorOf = (id: string) => roleColor.get(byId.get(id)!.group)!;

	// Narrow screens use the portrait layout; everything reads positions through posOf.
	const posOf = (n: SceneNode): Vec3 => (compact ? (MOBILE_POS[n.id] ?? n.pos) : n.pos);
	const anchor = (n: SceneNode): Vec3 => {
		const p = posOf(n);
		return [p[0], p[1] + (n.core ? 0.95 : 0.12), p[2]];
	};
	let edges: EdgeGeom[] = [];
	function buildEdges() {
		edges = EDGES.map((edge, i) => {
			const p0 = anchor(byId.get(edge.from)!);
			const p2 = anchor(byId.get(edge.to)!);
			const len = Math.hypot(p2[0] - p0[0], p2[1] - p0[1], p2[2] - p0[2]);
			const lift = FLOW[edge.kind].lift * Math.min(compact ? 1 : 1.6, len / 4);
			const p1: Vec3 = [(p0[0] + p2[0]) / 2, (p0[1] + p2[1]) / 2 + lift, (p0[2] + p2[2]) / 2];
			return { edge, p0, p1, p2, phase: (i * 0.37) % 1 };
		});
	}

	// Floor grid as short segments so each fades with distance from the core.
	const floor: [Vec3, Vec3, number][] = [];
	for (let gx = -13; gx <= 14; gx += 1) {
		for (let gz = -11; gz < 11; gz += 1) {
			const d = Math.hypot(gx - 1, (gz + 0.5 + 1) * 1.2);
			floor.push([[gx, -0.02, gz], [gx, -0.02, gz + 1], d]);
		}
	}
	for (let gz = -11; gz <= 11; gz += 1) {
		for (let gx = -13; gx < 14; gx += 1) {
			const d = Math.hypot(gx + 0.5 - 1, (gz + 1) * 1.2);
			floor.push([[gx, -0.02, gz], [gx + 1, -0.02, gz], d]);
		}
	}

	// Distant dust for depth.
	let seed = 7;
	const rand = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
	const dust: Vec3[] = Array.from({ length: 140 }, () => [rand() * 34 - 16, rand() * 12 - 1, rand() * 26 - 15]);

	let W = 0;
	let H = 0;
	let focal = 1;
	let target: Vec3 = [0.3, 1.0, -0.6];
	let dist = 27;
	let yaw = 0;
	let pitch = 0;
	let yawUser = 0;
	let pitchUser = 0;
	let camPos: Vec3 = [0, 0, 0];
	let cy = 1;
	let sy = 0;
	let cp = 1;
	let sp = 0;
	let compact: boolean | null = null;

	function project(p: Vec3): Proj {
		const dx = p[0] - target[0];
		const dy = p[1] - target[1];
		const dz = p[2] - target[2];
		const x1 = dx * cy - dz * sy;
		const z1 = dx * sy + dz * cy;
		const y2 = dy * cp - z1 * sp;
		const z2 = dy * sp + z1 * cp;
		const depth = Math.max(1, dist - z2);
		const s = dist / depth;
		return { x: W / 2 + (x1 * focal) / depth, y: H * (compact ? 0.4 : 0.6) - (y2 * focal) / depth, depth, s };
	}

	function setCamera(t: number) {
		const sway = reduceMotion ? 0 : Math.sin(t * 0.09) * 0.2;
		yaw = (compact ? 0 : 0.16) + sway * (compact ? 0.35 : 1) + yawUser;
		pitch = (compact ? 0.95 : 0.5) + (reduceMotion ? 0 : Math.sin(t * 0.06) * 0.04) + pitchUser;
		cy = Math.cos(yaw);
		sy = Math.sin(yaw);
		cp = Math.cos(pitch);
		sp = Math.sin(pitch);
		camPos = [target[0] + dist * cp * sy, target[1] + dist * sp, target[2] + dist * cp * cy];
	}

	function resize() {
		const rect = canvas!.getBoundingClientRect();
		const dpr = Math.min(window.devicePixelRatio || 1, 2);
		W = rect.width;
		H = rect.height;
		canvas!.width = Math.round(W * dpr);
		canvas!.height = Math.round(H * dpr);
		ctx!.setTransform(dpr, 0, 0, dpr, 0, 0);
		const nextCompact = W < 640;
		if (nextCompact !== compact) {
			compact = nextCompact;
			target = compact ? [0, 0.6, -0.4] : [0.3, 1.0, -0.6];
			dist = compact ? 38 : 27; // Flatter perspective keeps the portrait rows the same size.
			yawUser = 0;
			pitchUser = 0;
			buildEdges();
		}
		focal = (compact ? Math.min(W * 2.45, H * 1.32) : Math.min(W * 1.0, H * 2.15)) * (dist / 27);
		if (!running) draw(lastT);
	}

	// Interaction state.
	let hoverId: string | null = null;
	let pinnedId: string | null = null;
	let groupFocus: GroupId | null = null;
	const screen = new Map<string, Proj>();

	function focusSets(): { nodes: Set<string>; edges: Set<SceneEdge> } | null {
		const id = pinnedId ?? hoverId;
		if (id) {
			const nodes = new Set([id]);
			const es = new Set<SceneEdge>();
			for (const e of EDGES) {
				if (e.from === id || e.to === id) {
					es.add(e);
					nodes.add(e.from);
					nodes.add(e.to);
				}
			}
			return { nodes, edges: es };
		}
		if (groupFocus) {
			const nodes = new Set(NODES.filter((n) => n.group === groupFocus).map((n) => n.id));
			const es = new Set(EDGES.filter((e) => nodes.has(e.from) || nodes.has(e.to)));
			return { nodes, edges: es };
		}
		return null;
	}

	function drawPath(pts: Proj[]) {
		ctx!.beginPath();
		pts.forEach((p, i) => (i ? ctx!.lineTo(p.x, p.y) : ctx!.moveTo(p.x, p.y)));
	}

	function drawBackground(t: number) {
		const g = ctx!.createRadialGradient(W * 0.55, H * 0.5, 0, W * 0.55, H * 0.5, Math.max(W, H) * 0.75);
		g.addColorStop(0, '#24163F');
		g.addColorStop(0.55, '#170E2A');
		g.addColorStop(1, C.bg);
		ctx!.fillStyle = g;
		ctx!.fillRect(0, 0, W, H);

		for (let i = 0; i < dust.length; i++) {
			const p = project(dust[i]);
			const tw = reduceMotion ? 0.5 : 0.5 + 0.5 * Math.sin(t * 1.3 + i * 2.1);
			ctx!.fillStyle = rgba(C.l4, 0.08 + 0.22 * tw * Math.min(1, p.s));
			ctx!.fillRect(p.x, p.y, 1.4 * p.s, 1.4 * p.s);
		}
	}

	function drawFloor(t: number) {
		// One path per alpha band keeps the grid to a handful of strokes.
		const bands = 8;
		const paths = Array.from({ length: bands }, () => new Path2D());
		for (const [a, b, d] of floor) {
			const k = Math.round(bands * Math.max(0, 1 - d / 15));
			if (k <= 0) continue;
			const pa = project(a);
			const pb = project(b);
			paths[k - 1].moveTo(pa.x, pa.y);
			paths[k - 1].lineTo(pb.x, pb.y);
		}
		ctx!.lineWidth = 1;
		paths.forEach((path, i) => {
			ctx!.strokeStyle = rgba(C.l2, (0.16 * (i + 1)) / bands);
			ctx!.stroke(path);
		});

		// HUD rings around the lakehouse core.
		const core = posOf(byId.get('lakehouse')!);
		const ring = (r: number, from: number, to: number, color: string, alpha: number, width: number, steps = 64) => {
			const pts: Proj[] = [];
			for (let i = 0; i <= steps; i++) {
				const a = from + ((to - from) * i) / steps;
				pts.push(project([core[0] + Math.cos(a) * r, 0, core[2] + Math.sin(a) * r]));
			}
			ctx!.strokeStyle = rgba(color, alpha);
			ctx!.lineWidth = width;
			drawPath(pts);
			ctx!.stroke();
		};
		ring(2.3, 0, Math.PI * 2, C.l3, 0.35, 1.2, 96);
		const spin = reduceMotion ? 0 : t * 0.25;
		for (let k = 0; k < 6; k++) {
			const a = spin + (k * Math.PI) / 3;
			ring(3.1, a, a + 0.62, C.l3, 0.55, 2.2, 16);
		}
		for (let k = 0; k < 3; k++) {
			const a = -spin * 0.6 + (k * 2 * Math.PI) / 3;
			ring(4.0, a, a + 1.4, C.dot, 0.35, 1.2, 24);
		}
		ring(5.4, 0, Math.PI * 2, C.l2, 0.18, 1, 120);
		// Tick marks on the outer ring.
		ctx!.strokeStyle = rgba(C.l3, 0.3);
		ctx!.lineWidth = 1;
		for (let k = 0; k < 72; k++) {
			const a = (k * Math.PI) / 36 + spin * 0.15;
			const r2 = k % 6 === 0 ? 5.9 : 5.6;
			const p1 = project([core[0] + Math.cos(a) * 5.4, 0, core[2] + Math.sin(a) * 5.4]);
			const p2 = project([core[0] + Math.cos(a) * r2, 0, core[2] + Math.sin(a) * r2]);
			ctx!.beginPath();
			ctx!.moveTo(p1.x, p1.y);
			ctx!.lineTo(p2.x, p2.y);
			ctx!.stroke();
		}
	}

	function drawEdges(t: number, focus: ReturnType<typeof focusSets>) {
		for (const e of edges) {
			const st = FLOW[e.edge.kind];
			const on = !focus || focus.edges.has(e.edge);
			const pts: Proj[] = [];
			for (let i = 0; i <= 28; i++) pts.push(project(bezier(e, i / 28)));
			const alpha = on ? Math.min(1, st.alpha * (focus ? 1.5 : 1)) : 0.06;
			const grad = ctx!.createLinearGradient(pts[0].x, pts[0].y, pts[28].x, pts[28].y);
			grad.addColorStop(0, rgba(colorOf(e.edge.from), alpha));
			grad.addColorStop(1, rgba(colorOf(e.edge.to), alpha));
			ctx!.setLineDash(st.dash);
			ctx!.lineDashOffset = reduceMotion ? 0 : -t * 14;
			ctx!.strokeStyle = grad;
			ctx!.lineWidth = st.width * (focus && on ? 1.5 : 1);
			drawPath(pts);
			ctx!.stroke();
		}
		ctx!.setLineDash([]);

		ctx!.globalCompositeOperation = 'lighter';
		for (const e of edges) {
			const st = FLOW[e.edge.kind];
			const on = !focus || focus.edges.has(e.edge);
			if (!on) continue;
			const sprite = e.edge.kind === 'stream' ? glow.ink : roleGlow.get(byId.get(e.edge.from)!.group)!;
			for (let k = 0; k < st.count; k++) {
				const u = (t * st.speed + e.phase + k / st.count) % 1;
				const p = project(bezier(e, u));
				const fade = Math.sin(u * Math.PI);
				const size = (e.edge.kind === 'stream' ? 16 : 14) * p.s * (0.6 + 0.4 * fade);
				ctx!.globalAlpha = 0.35 + 0.65 * fade;
				ctx!.drawImage(sprite, p.x - size / 2, p.y - size / 2, size, size);
			}
		}
		ctx!.globalAlpha = 1;
		ctx!.globalCompositeOperation = 'source-over';
	}

	function drawNode(n: SceneNode, t: number, dim: boolean, hot: boolean) {
		const [x, y, z] = posOf(n);
		const r = 0.62;
		const h = 0.26;
		const a = dim ? 0.22 : 1;
		const col = roleColor.get(n.group)!;
		const bob = reduceMotion || y === 0 ? 0 : Math.sin(t * 0.8 + x) * 0.08;
		const yy = y + bob;

		if (y > 0) {
			// Tether to the floor so raised nodes read in depth.
			const top = project([x, yy, z]);
			const base = project([x, 0, z]);
			ctx!.setLineDash([2, 4]);
			ctx!.strokeStyle = rgba(col, 0.35 * a);
			ctx!.lineWidth = 1;
			ctx!.beginPath();
			ctx!.moveTo(top.x, top.y);
			ctx!.lineTo(base.x, base.y);
			ctx!.stroke();
			ctx!.setLineDash([]);
			ctx!.fillStyle = rgba(col, 0.4 * a);
			ctx!.beginPath();
			ctx!.ellipse(base.x, base.y, 6 * base.s, 2.4 * base.s, 0, 0, Math.PI * 2);
			ctx!.fill();
		}

		const ang = (i: number) => (i * Math.PI) / 3 + Math.PI / 6;
		const bottom: Proj[] = [];
		const top: Proj[] = [];
		for (let i = 0; i < 6; i++) {
			const cx = x + Math.cos(ang(i)) * r;
			const cz = z + Math.sin(ang(i)) * r;
			bottom.push(project([cx, yy, cz]));
			top.push(project([cx, yy + h, cz]));
		}
		const center = project([x, yy + h, z]);

		ctx!.globalCompositeOperation = 'lighter';
		const gs = (hot ? 120 : 84) * center.s;
		ctx!.globalAlpha = (hot ? 0.75 : 0.4) * a;
		ctx!.drawImage(roleGlow.get(n.group)!, center.x - gs / 2, center.y - gs / 2, gs, gs);
		ctx!.globalAlpha = 1;
		ctx!.globalCompositeOperation = 'source-over';

		for (let i = 0; i < 6; i++) {
			const mid = ang(i) + Math.PI / 6;
			const nx = Math.cos(mid);
			const nz = Math.sin(mid);
			const vis = nx * (camPos[0] - x) + nz * (camPos[2] - z);
			if (vis <= 0) continue;
			const j = (i + 1) % 6;
			const side = [bottom[i], bottom[j], top[j], top[i]];
			ctx!.fillStyle = rgba(C.b1, 0.94 * a);
			drawPath(side);
			ctx!.closePath();
			ctx!.fill();
			ctx!.fillStyle = rgba(col, 0.16 * a);
			ctx!.strokeStyle = rgba(col, 0.6 * a);
			ctx!.lineWidth = 1;
			ctx!.fill();
			ctx!.stroke();
		}

		const g = ctx!.createLinearGradient(top[3].x, top[3].y, top[0].x, top[0].y);
		g.addColorStop(0, rgba(col, 0.34 * a));
		g.addColorStop(1, rgba(col, 0.12 * a));
		ctx!.fillStyle = rgba(C.c0, 0.98 * a);
		drawPath(top);
		ctx!.closePath();
		ctx!.fill();
		ctx!.fillStyle = g;
		ctx!.strokeStyle = rgba(hot ? '#FFFFFF' : col, (hot ? 1 : 0.9) * a);
		ctx!.lineWidth = hot ? 1.8 : 1.3;
		ctx!.fill();
		ctx!.stroke();

		ctx!.textAlign = 'center';
		ctx!.textBaseline = 'middle';
		ctx!.font = `700 ${Math.max(7, 10 * center.s)}px ${FONT}`;
		ctx!.fillStyle = rgba(col, a);
		ctx!.fillText(n.glyph, center.x, center.y + 0.5);

		screen.set(n.id, { ...center, s: Math.max(18, 34 * center.s) });
		labels.push({ n, at: project([x, yy + h + 0.45, z]), a, hot });
	}

	function drawCore(n: SceneNode, t: number, dim: boolean, hot: boolean) {
		const [x, , z] = posOf(n);
		const a = dim ? 0.3 : 1;
		const spin = reduceMotion ? 0.6 : t * 0.5;

		// Light column.
		const base = project([x, 0, z]);
		const tip = project([x, 3.2, z]);
		const col = ctx!.createLinearGradient(base.x, base.y, tip.x, tip.y);
		col.addColorStop(0, rgba(C.dot, 0.5 * a));
		col.addColorStop(1, rgba(C.dot, 0));
		ctx!.strokeStyle = col;
		ctx!.lineWidth = 3 * base.s;
		ctx!.beginPath();
		ctx!.moveTo(base.x, base.y);
		ctx!.lineTo(tip.x, tip.y);
		ctx!.stroke();

		LAYERS.forEach((L, li) => {
			const showName = !compact || li === 2;
			const pts: Proj[] = [];
			for (let i = 0; i <= 72; i++) {
				const ang = (i / 72) * Math.PI * 2;
				pts.push(project([x + Math.cos(ang) * L.r, L.y, z + Math.sin(ang) * L.r]));
			}
			ctx!.fillStyle = rgba(L.color, (li === 2 ? 0.2 : 0.13) * a);
			ctx!.strokeStyle = rgba(L.color, (hot ? 0.95 : 0.7) * a);
			ctx!.lineWidth = li === 2 ? 1.8 : 1.3;
			drawPath(pts);
			ctx!.closePath();
			ctx!.fill();
			ctx!.stroke();

			// Bright arc sweeping around each layer.
			const dir = li % 2 ? -1 : 1;
			const s0 = spin * dir * (1 + li * 0.3);
			const arc: Proj[] = [];
			for (let i = 0; i <= 20; i++) {
				const ang = s0 + (i / 20) * 1.1;
				arc.push(project([x + Math.cos(ang) * L.r, L.y, z + Math.sin(ang) * L.r]));
			}
			ctx!.strokeStyle = rgba(li === 2 ? C.dot : C.l4, 0.9 * a);
			ctx!.lineWidth = 2.6;
			drawPath(arc);
			ctx!.stroke();

			// Data tiles orbiting inside each layer.
			ctx!.globalCompositeOperation = 'lighter';
			for (let k = 0; k < 10; k++) {
				const ang = -s0 * 0.7 + (k * Math.PI) / 5;
				const p = project([x + Math.cos(ang) * L.r * 0.62, L.y, z + Math.sin(ang) * L.r * 0.62]);
				const sz = 10 * p.s;
				ctx!.globalAlpha = 0.7 * a;
				ctx!.drawImage(li === 2 ? glow.pink : glow.ink, p.x - sz / 2, p.y - sz / 2, sz, sz);
			}
			ctx!.globalAlpha = 1;
			ctx!.globalCompositeOperation = 'source-over';

			if (!showName) return;
			const lp = project([x + L.r + 0.35, L.y, z]);
			ctx!.textAlign = 'left';
			ctx!.textBaseline = 'middle';
			ctx!.font = `700 ${Math.max(9, 12 * lp.s)}px ${FONT}`;
			ctx!.fillStyle = rgba(li === 2 ? C.dot : C.l4, a);
			ctx!.fillText(L.name, lp.x, lp.y);
		});

		const center = project([x, 0.95, z]);
		ctx!.globalCompositeOperation = 'lighter';
		const gs = 190 * center.s;
		ctx!.globalAlpha = 0.45 * a;
		ctx!.drawImage(glow.pink, center.x - gs / 2, center.y - gs / 2, gs, gs);
		ctx!.globalAlpha = 1;
		ctx!.globalCompositeOperation = 'source-over';

		screen.set(n.id, { ...center, s: Math.max(40, 70 * center.s) });
		labels.push({ n, at: project([x, 2.45, z]), a, hot, core: true });
	}

	let labels: { n: SceneNode; at: Proj; a: number; hot: boolean; core?: boolean }[] = [];

	function drawLabels() {
		labels.sort((p, q) => q.at.depth - p.at.depth);
		for (const { n, at, a, hot, core } of labels) {
			const size = compact ? Math.max(9, (core ? 13 : 10.5) * at.s) : Math.max(10, (core ? 15 : 12.5) * at.s);
			ctx!.textAlign = 'center';
			ctx!.textBaseline = 'bottom';
			ctx!.font = `700 ${size}px ${FONT}`;
			ctx!.lineWidth = 4;
			ctx!.lineJoin = 'round';
			ctx!.strokeStyle = rgba(C.bg, 0.85 * a);
			ctx!.strokeText(n.label, at.x, at.y);
			ctx!.fillStyle = rgba(hot ? '#FFFFFF' : C.ink, a);
			ctx!.fillText(n.label, at.x, at.y);
		}
	}

	let lastT = 0;
	function draw(t: number) {
		lastT = t;
		setCamera(t);
		screen.clear();
		labels = [];
		const focus = focusSets();
		drawBackground(t);
		drawFloor(t);
		drawEdges(t, focus);

		const depth = new Map(NODES.map((n) => [n.id, project(posOf(n)).depth]));
		const order = [...NODES].sort((p, q) => depth.get(q.id)! - depth.get(p.id)!);
		const active = pinnedId ?? hoverId;
		for (const n of order) {
			const dim = !!focus && !focus.nodes.has(n.id);
			const hot = active === n.id || (!!groupFocus && n.group === groupFocus);
			if (n.core) drawCore(n, t, dim, hot);
			else drawNode(n, t, dim, hot);
		}
		drawLabels();
		placeCard();
	}

	function placeCard() {
		const id = pinnedId ?? hoverId;
		const p = id ? screen.get(id) : undefined;
		if (!id || !p) {
			card!.hidden = true;
			return;
		}
		const n = byId.get(id)!;
		if (card!.dataset.id !== id) {
			card!.dataset.id = id;
			card!.querySelector('[data-card-group]')!.textContent = groupLabel.get(n.group) ?? '';
			card!.querySelector('[data-card-title]')!.textContent = n.label;
			card!.querySelector('[data-card-detail]')!.textContent = n.detail;
		}
		card!.hidden = false;
		const cw = card!.offsetWidth;
		const ch = card!.offsetHeight;
		let left = p.x + p.s * 0.8 + 12;
		if (left + cw > W - 8) left = p.x - p.s * 0.8 - 12 - cw;
		left = Math.max(8, Math.min(W - cw - 8, left));
		const top = Math.max(8, Math.min(H - ch - 8, p.y - ch / 2));
		// The card is positioned in the stage; the canvas may sit below the header (narrow layout).
		card!.style.transform = `translate(${Math.round(left + canvas!.offsetLeft)}px, ${Math.round(top + canvas!.offsetTop)}px)`;
	}

	function hit(mx: number, my: number): string | null {
		let best: string | null = null;
		let bestD = Number.POSITIVE_INFINITY;
		for (const [id, p] of screen) {
			const d = Math.hypot(mx - p.x, my - p.y);
			if (d < p.s && d < bestD) {
				best = id;
				bestD = d;
			}
		}
		return best;
	}

	// Pointer: drag to orbit, hover or tap to inspect.
	let drag: { x: number; y: number; moved: boolean } | null = null;
	const endDrag = () => {
		drag = null;
		canvas.classList.remove('is-dragging');
	};
	canvas.addEventListener('pointerdown', (ev) => {
		if (ev.button !== 0) return;
		drag = { x: ev.clientX, y: ev.clientY, moved: false };
		if (ev.pointerType === 'mouse') canvas.setPointerCapture(ev.pointerId);
	});
	canvas.addEventListener('pointerup', (ev) => {
		if (drag && !drag.moved) {
			const r = canvas.getBoundingClientRect();
			const id = hit(ev.clientX - r.left, ev.clientY - r.top);
			pinnedId = id && id !== pinnedId ? id : null;
			requestDraw();
		}
		endDrag();
	});
	canvas.addEventListener('pointercancel', endDrag);
	canvas.addEventListener('lostpointercapture', endDrag);
	canvas.addEventListener('pointermove', (ev) => {
		const r = canvas.getBoundingClientRect();
		if (drag && ev.pointerType === 'mouse' && ev.buttons === 0) endDrag();
		if (drag) {
			const dx = ev.clientX - drag.x;
			const dy = ev.clientY - drag.y;
			if (Math.abs(dx) + Math.abs(dy) > 4) {
				drag.moved = true;
				canvas.classList.add('is-dragging');
			}
			if (drag.moved) {
				yawUser = Math.max(-0.7, Math.min(0.7, yawUser - dx * 0.005));
				pitchUser = Math.max(-0.3, Math.min(0.45, pitchUser + dy * 0.003));
				drag.x = ev.clientX;
				drag.y = ev.clientY;
			}
		}
		if (ev.pointerType === 'mouse') {
			const id = hit(ev.clientX - r.left, ev.clientY - r.top);
			if (id !== hoverId) {
				hoverId = id;
				canvas.style.cursor = id ? 'pointer' : '';
			}
		}
		requestDraw();
	});
	canvas.addEventListener('pointerleave', () => {
		if (drag) return;
		hoverId = null;
		requestDraw();
	});

	for (const chip of root.querySelectorAll<HTMLButtonElement>('[data-group]')) {
		const g = chip.dataset.group as GroupId;
		const on = () => {
			groupFocus = g;
			requestDraw();
		};
		const off = () => {
			if (chip.getAttribute('aria-pressed') !== 'true') groupFocus = pinnedGroup();
			requestDraw();
		};
		chip.addEventListener('pointerenter', on);
		chip.addEventListener('focus', on);
		chip.addEventListener('pointerleave', off);
		chip.addEventListener('blur', off);
		chip.addEventListener('click', () => {
			const pressed = chip.getAttribute('aria-pressed') === 'true';
			for (const other of root.querySelectorAll('[data-group]')) other.setAttribute('aria-pressed', 'false');
			chip.setAttribute('aria-pressed', String(!pressed));
			groupFocus = pressed ? null : g;
			pinnedId = null;
			requestDraw();
		});
	}
	function pinnedGroup(): GroupId | null {
		const pressed = root.querySelector<HTMLElement>('[data-group][aria-pressed="true"]');
		return (pressed?.dataset.group as GroupId | undefined) ?? null;
	}

	// Animation loop, paused off-screen and for reduced motion.
	let running = false;
	let visible = true;
	let raf = 0;
	const t0 = performance.now();
	let lastFrame = 0;
	function loop(now: number) {
		raf = requestAnimationFrame(loop);
		if (now - lastFrame < 15) return; // Cap at ~60 fps on high-refresh displays.
		lastFrame = now;
		draw((now - t0) / 1000);
	}
	function sync() {
		const should = visible && !document.hidden && !reduceMotion;
		if (should && !running) {
			running = true;
			raf = requestAnimationFrame(loop);
		} else if (!should && running) {
			running = false;
			cancelAnimationFrame(raf);
		}
	}
	function requestDraw() {
		if (!running) draw(reduceMotion ? 4 : lastT);
	}

	new ResizeObserver(resize).observe(canvas);
	new IntersectionObserver((entries) => {
		visible = entries.some((e) => e.isIntersecting);
		sync();
	}).observe(root);
	document.addEventListener('visibilitychange', sync);
	// The canvas does not trigger font loading by itself; request Urbanist and redraw.
	Promise.all([document.fonts?.load(`700 12px ${FONT}`), document.fonts?.load(`500 12px ${FONT}`)])
		.then(() => requestDraw())
		.catch(() => undefined);

	resize();
	requestDraw();
	sync();
}
