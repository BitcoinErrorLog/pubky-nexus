import {
  defineRailway,
  preserve,
  project,
  service,
  volume,
} from "railway/iac";

export const partial = "nexusd";

const PRODUCTION_PROJECT_ID = "af82731f-a6d0-4c0e-84cd-56ce6fcc8818";

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

export default defineRailway((ctx) => {
  if (ctx.projectId !== PRODUCTION_PROJECT_ID) {
    throw new Error(
      `Unknown Railway project ${ctx.projectId ?? "(none)"}. This file covers pubky-marketplace-nexus (${PRODUCTION_PROJECT_ID}).`,
    );
  }

  const nexusdVolume = volume("nexusd-volume", {
    region: "us-west2",
    sizeMB: 50_000,
  });

  const nexusd = service("nexusd", {
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
