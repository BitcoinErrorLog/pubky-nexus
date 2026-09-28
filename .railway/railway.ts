import {
  defineRailway,
  image,
  preserve,
  project,
  service,
  volume,
} from "railway/iac";

export const partial = "nexusd";

const PRODUCTION_PROJECT_ID = "af82731f-a6d0-4c0e-84cd-56ce6fcc8818";
const PRODUCTION_ENVIRONMENT_ID = "aa35d5df-634d-4cdd-9e53-49b0d9c73efc";

// Pinned to the live GHCR digest. Bump in the same PR as each IMAGE connect.
const LIVE_IMAGE =
  "ghcr.io/bitcoinerrorlog/pubky-nexus@sha256:8498ef94de425512fada9e361fd00975f1902073756456d71b62881e44f3dcca";

const operationalEnv = {
  NEXUS_EVENTS_LIMIT: preserve(),
  NEXUS_HOMESERVER: preserve(),
  NEXUS_NEO4J_PASSWORD: preserve(),
  NEXUS_NEO4J_URI: preserve(),
  NEXUS_REDIS_URL: preserve(),
  NEXUS_TESTNET: preserve(),
  NEXUS_WATCHER_SLEEP: preserve(),
  PORT: preserve(),
};

function resolvedEnvironmentId(ctx: { environmentId?: string }): string | undefined {
  return ctx.environmentId ?? process.env.RAILWAY_ENVIRONMENT_ID;
}

export default defineRailway((ctx) => {
  if (ctx.projectId !== PRODUCTION_PROJECT_ID) {
    throw new Error(
      `Unknown Railway project ${ctx.projectId ?? "(none)"}. This file covers pubky-marketplace-nexus (${PRODUCTION_PROJECT_ID}).`,
    );
  }
  const environmentId = resolvedEnvironmentId(ctx);
  if (environmentId && environmentId !== PRODUCTION_ENVIRONMENT_ID) {
    throw new Error(
      `Refuse to evaluate the production graph against environment ${environmentId}. Set RAILWAY_ENVIRONMENT_ID=${PRODUCTION_ENVIRONMENT_ID}.`,
    );
  }

  const nexusdVolume = volume("nexusd-volume", {
    region: "us-west2",
    sizeMB: 50_000,
  });

  const nexusd = service("nexusd", {
    source: image(LIVE_IMAGE),
    // Live production still records builder DOCKERFILE after IMAGE connect.
    build: {
      builder: "DOCKERFILE",
      dockerfilePath: "Dockerfile.railway",
    },
    env: operationalEnv,
    volumeMounts: {
      "/data": nexusdVolume,
    },
  });

  return project("pubky-marketplace-nexus", {
    resources: [nexusd, nexusdVolume],
  });
});
