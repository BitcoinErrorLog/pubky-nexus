import {
  defineRailway,
  preserve,
  project,
  service,
  volume,
} from "railway/iac";

export const partial = "nexusd";

const PRODUCTION_PROJECT_ID = "75faa4fe-466c-4277-977f-1d8e4e31df8c";

const operationalEnv = {
  NEXUS_EVENTS_LIMIT: preserve(),
  NEXUS_HOMESERVER: preserve(),
  NEXUS_NEO4J_PASSWORD: preserve(),
  NEXUS_NEO4J_URI: preserve(),
  NEXUS_REDIS_URL: preserve(),
  NEXUS_TESTNET: preserve(),
  NEXUS_WATCHER_SLEEP: preserve(),
  PORT: preserve(),
  RAILWAY_DOCKERFILE_PATH: preserve(),
};

export default defineRailway((ctx) => {
  if (ctx.projectId !== PRODUCTION_PROJECT_ID) {
    throw new Error(
      `Unknown Railway project ${ctx.projectId ?? "(none)"}. This file covers pubky-marketplace-production (${PRODUCTION_PROJECT_ID}).`,
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

  return project("pubky-marketplace-production", {
    resources: [nexusd, nexusdVolume],
  });
});
