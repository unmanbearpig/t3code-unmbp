import Constants from "expo-constants";
import * as Crypto from "expo-crypto";
import { PROVIDER_SEND_TURN_MAX_FILE_BYTES } from "@t3tools/contracts";
import {
  clearSharedPayloads,
  getResolvedSharedPayloadsAsync,
  getSharedPayloads,
  type ResolvedSharePayload,
  type SharePayload,
} from "expo-sharing";
import React, { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from "react";
import { Alert, AppState, Platform } from "react-native";

import {
  buildIncomingShareDraft,
  isShareFileUriUnderOwnedRoots,
  type IncomingShareDestination,
  type IncomingShareDraft,
} from "./incoming-share-model";
import { createIncomingSharePayloadReader } from "./incoming-share-native";
import { describeError, describeUri, shareDebug } from "./share-debug";
import { IncomingShareInbox } from "./incoming-share-inbox";
import { persistComposerAttachmentFile } from "../../lib/composerImages";
import {
  loadIncomingShareDrafts,
  removeIncomingShareDraft,
  writeIncomingShareDraft,
} from "./incoming-share-storage";

type IncomingShareContextValue = {
  readonly pendingShare: IncomingShareDraft | null;
  readonly isLoading: boolean;
  readonly error: Error | null;
  readonly getShare: (shareId: string) => IncomingShareDraft | null;
  readonly reserveShare: (shareId: string, destination: IncomingShareDestination) => Promise<void>;
  readonly releaseShareReservation: (
    shareId: string,
    expectedDestination: IncomingShareDestination,
  ) => Promise<void>;
  readonly consumeShare: (shareId: string) => Promise<void>;
  readonly refresh: () => Promise<void>;
};

const IncomingShareContext = React.createContext<IncomingShareContextValue | null>(null);

function receiveSharingEnabled(): boolean {
  if (Platform.OS === "android") {
    return true;
  }
  if (Platform.OS !== "ios") {
    return false;
  }
  return Constants.expoConfig?.extra?.iosPersonalTeamBuild !== true;
}

const getIncomingSharePayloads = createIncomingSharePayloadReader({
  platform: Platform.OS,
  readPayloads: () => {
    const payloads = getSharedPayloads();
    if (payloads.length > 0) {
      shareDebug(
        "incoming:native payloads",
        payloads.map((payload) => ({
          shareType: payload.shareType,
          mimeType: payload.mimeType,
          originalName: payload.originalName,
          value: payload.shareType === "text" ? "<text>" : describeUri(payload.value),
        })),
      );
    }
    return payloads;
  },
});

function describeDraft(draft: IncomingShareDraft) {
  return {
    id: draft.id.slice(0, 18),
    destination: draft.destination ?? null,
    textLength: draft.text.length,
    attachments: draft.attachments.map((attachment) => ({
      type: attachment.type,
      name: attachment.name,
      sizeBytes: "sizeBytes" in attachment ? attachment.sizeBytes : undefined,
    })),
    warnings: draft.warnings,
  };
}

async function resolvedPayloadsForFiles(): Promise<ReadonlyArray<ResolvedSharePayload>> {
  try {
    return await getResolvedSharedPayloadsAsync();
  } catch (error) {
    // iOS already gives the containing app a copied file:// URL, so raw
    // payloads remain usable. Android normally resolves content:// into a
    // private cache file; its modern File API can still read the raw URI when
    // resolution fails.
    console.warn("[incoming-share] could not resolve shared file metadata", error);
    return [];
  }
}

async function incomingShareIdForPayloads(payloads: ReadonlyArray<SharePayload>): Promise<string> {
  const fingerprint = JSON.stringify(
    payloads.map((payload) => ({
      shareType: payload.shareType,
      mimeType: payload.mimeType ?? null,
      value: payload.value,
    })),
  );
  const digest = await Crypto.digestStringAsync(Crypto.CryptoDigestAlgorithm.SHA256, fingerprint);
  shareDebug("incoming:share id", `share-${digest}`.slice(0, 18));
  return `share-${digest}`;
}

async function readBase64(uri: string): Promise<string> {
  const { File } = await import("expo-file-system");
  return new File(uri).base64();
}

async function readFileSize(uri: string): Promise<number | null> {
  const { File } = await import("expo-file-system");
  try {
    const size = new File(uri).size ?? null;
    shareDebug("incoming:readSize", { uri: describeUri(uri), size });
    return size;
  } catch (error) {
    shareDebug("incoming:readSize failed", { uri: describeUri(uri), error: describeError(error) });
    throw error;
  }
}

async function removeOwnedFile(uri: string): Promise<void> {
  if (!uri.startsWith("file:")) {
    return;
  }
  try {
    const { File, Paths } = await import("expo-file-system");
    // Only delete files in directories this app owns: its documents and cache
    // sandbox and its share-extension App Group container. An iOS
    // open-in-place share points at the sender's own storage; deleting that
    // URI would destroy the user's document.
    const ownedRootUris = [
      Paths.document.uri,
      Paths.cache.uri,
      ...Object.values(Paths.appleSharedContainers ?? {}).map((directory) => directory.uri),
    ];
    if (!isShareFileUriUnderOwnedRoots(uri, ownedRootUris)) {
      return;
    }
    const file = new File(uri);
    if (file.exists) {
      file.delete();
    }
  } catch (error) {
    console.warn("[incoming-share] could not remove temporary shared file", error);
  }
}

async function removeReplayedPayloadFiles(payloads: ReadonlyArray<SharePayload>): Promise<void> {
  const uris = new Set<string>();
  for (const payload of payloads) {
    if (["image", "file", "audio", "video"].includes(payload.shareType)) {
      uris.add(payload.value);
    }
  }
  if (uris.size === 0) {
    return;
  }
  const resolvedPayloads = payloads.some((payload) =>
    ["file", "audio", "video"].includes(payload.shareType),
  )
    ? []
    : await resolvedPayloadsForFiles();
  for (const payload of resolvedPayloads) {
    if (["image", "file", "audio", "video"].includes(payload.shareType) && payload.contentUri) {
      uris.add(payload.contentUri);
    }
  }
  await Promise.all([...uris].map(removeOwnedFile));
}

// Keep one operation queue across provider remounts (including development
// Strict Mode remounts) so two app lifecycle notifications cannot ingest the
// same native handoff independently.
const incomingShareInbox = new IncomingShareInbox({
  loadDrafts: loadIncomingShareDrafts,
  writeDraft: writeIncomingShareDraft,
  removeDraft: removeIncomingShareDraft,
  getPayloads: getIncomingSharePayloads,
  clearPayloads: clearSharedPayloads,
  buildDraft: async ({ payloads, id, createdAt }) => {
    const cleanupUris = new Set<string>();
    const persistedUris = new Set<string>();
    const hasGenericFilePayload = payloads.some((payload) =>
      ["file", "audio", "video"].includes(payload.shareType),
    );
    const resolvedPayloads =
      !hasGenericFilePayload && payloads.some((payload) => payload.shareType === "image")
        ? await resolvedPayloadsForFiles()
        : [];
    const draft = await buildIncomingShareDraft({
      payloads,
      resolvedPayloads,
      fileReader: {
        readBase64,
        persistFile: async (uri, name) => {
          let persistedUri: string;
          try {
            persistedUri = await persistComposerAttachmentFile(
              uri,
              name,
              PROVIDER_SEND_TURN_MAX_FILE_BYTES,
            );
          } catch (error) {
            shareDebug("incoming:persistFile failed", {
              uri: describeUri(uri),
              name,
              error: describeError(error),
            });
            throw error;
          }
          shareDebug("incoming:persistFile", {
            from: describeUri(uri),
            to: describeUri(persistedUri),
          });
          persistedUris.add(persistedUri);
          return persistedUri;
        },
        readSize: readFileSize,
        removeOwnedFile: (uri) => {
          cleanupUris.add(uri);
        },
      },
      id,
      createdAt,
    });
    shareDebug("incoming:built draft", describeDraft(draft));
    return {
      draft,
      cleanup: async () => {
        await Promise.all([...cleanupUris].map(removeOwnedFile));
      },
      rollback: async () => {
        await Promise.all([...persistedUris].map(removeOwnedFile));
      },
    };
  },
  cleanupReplayedPayloads: async (payloads) => {
    shareDebug("incoming:replayed payload, existing inbox draft kept");
    await removeReplayedPayloadFiles(payloads);
  },
  idForPayloads: incomingShareIdForPayloads,
  now: () => new Date().toISOString(),
  onClearError: (error) => {
    console.warn("[incoming-share] could not acknowledge native payload", error);
  },
  onCleanupError: (error) => {
    console.warn("[incoming-share] could not remove temporary shared file", error);
  },
});

export function IncomingShareProvider(props: React.PropsWithChildren) {
  const enabled = receiveSharingEnabled();
  const [drafts, setDrafts] = useState<ReadonlyArray<IncomingShareDraft>>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const refreshPromiseRef = useRef<Promise<void> | null>(null);
  const mountedRef = useRef(false);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const refresh = useCallback(async () => {
    if (refreshPromiseRef.current) {
      return refreshPromiseRef.current;
    }

    const operation = (async () => {
      try {
        const snapshot = await incomingShareInbox.refresh({ ingestNative: enabled });
        shareDebug("inbox:refresh", snapshot.map(describeDraft));
        if (mountedRef.current) {
          setDrafts(snapshot);
          setError(null);
        }
      } catch (cause) {
        shareDebug("inbox:refresh failed", describeError(cause));
        const persisted = await incomingShareInbox
          .refresh({ ingestNative: false })
          .catch(() => null);
        if (mountedRef.current) {
          if (persisted) {
            setDrafts(persisted);
          }
          setError(cause instanceof Error ? cause : new Error("Could not import shared content."));
        }
      }
    })().finally(() => {
      refreshPromiseRef.current = null;
    });

    refreshPromiseRef.current = operation;
    return operation;
  }, [enabled]);

  useEffect(() => {
    void refresh().finally(() => {
      if (mountedRef.current) {
        setIsLoading(false);
      }
    });
  }, [refresh]);

  const refreshOnAppActive = useEffectEvent(() => {
    void refresh();
  });

  useEffect(() => {
    if (!enabled) {
      return;
    }
    const subscription = AppState.addEventListener("change", (state) => {
      shareDebug("appState", state);
      if (state === "active") {
        refreshOnAppActive();
      }
    });
    return () => subscription.remove();
  }, [enabled]);

  useEffect(() => {
    if (!error) {
      return;
    }
    Alert.alert("Could not import shared content", error.message, [
      { text: "Dismiss", style: "cancel", onPress: () => setError(null) },
      {
        text: "Retry",
        onPress: () => {
          setError(null);
          void refresh();
        },
      },
    ]);
  }, [error, refresh]);

  const consumeShare = useCallback(async (shareId: string) => {
    shareDebug("inbox:consume", shareId.slice(0, 18));
    const snapshot = await incomingShareInbox.consume(shareId);
    if (mountedRef.current) {
      setDrafts(snapshot);
    }
  }, []);
  const reserveShare = useCallback(
    async (shareId: string, destination: IncomingShareDestination) => {
      shareDebug("inbox:reserve", { shareId: shareId.slice(0, 18), destination });
      const snapshot = await incomingShareInbox.reserve(shareId, destination);
      if (mountedRef.current) {
        setDrafts(snapshot);
      }
    },
    [],
  );
  const releaseShareReservation = useCallback(
    async (shareId: string, expectedDestination: IncomingShareDestination) => {
      shareDebug("inbox:releaseReservation", shareId.slice(0, 18));
      const snapshot = await incomingShareInbox.releaseReservation(shareId, expectedDestination);
      if (mountedRef.current) {
        setDrafts(snapshot);
      }
    },
    [],
  );
  const getShare = useCallback(
    (shareId: string) => drafts.find((draft) => draft.id === shareId) ?? null,
    [drafts],
  );

  const value = useMemo<IncomingShareContextValue>(
    () => ({
      pendingShare: drafts[0] ?? null,
      isLoading,
      error,
      getShare,
      releaseShareReservation,
      reserveShare,
      consumeShare,
      refresh,
    }),
    [
      consumeShare,
      drafts,
      error,
      getShare,
      isLoading,
      refresh,
      releaseShareReservation,
      reserveShare,
    ],
  );

  return (
    <IncomingShareContext.Provider value={value}>{props.children}</IncomingShareContext.Provider>
  );
}

export function useIncomingShare(): IncomingShareContextValue {
  const value = React.use(IncomingShareContext);
  if (value === null) {
    throw new Error("useIncomingShare must be used within IncomingShareProvider.");
  }
  return value;
}
