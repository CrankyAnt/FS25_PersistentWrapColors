# Chain the visual area update through superFunc

Up to version 1.1.1.0 the mod replaced `PlaceableObjectStorage.updateObjectStorageVisualAreas` completely and never called the game function, which broke other mods that extend Object Storage, such as Digital Display ([issue #1](https://github.com/CrankyAnt/FS25_PersistentWrapColors/issues/1)). Since version 1.1.2.0 the mod patches that function with `Utils.overwrittenFunction`, calls `superFunc` first and colors the visuals afterwards, because compatibility with other Object Storage mods weighs more than a clean spawn path of its own.

## Considered Options

- Replace the function completely and spawn colored visuals in one pass: simple and without double work, but every other mod that patches the function stops working.
- Call `superFunc`, then take over the remaining spawn steps of the visual area update: compatible with other mods, at the cost of the work described below.

## Consequences

- The game has usually spawned the visuals of the first object info before `superFunc` returns. Where server and client run in one game, as in singleplayer, the mod deletes those uncolored visuals and spawns them again with their wrapping colors; a multiplayer client colors them afterwards. Do not remove this step as redundant.
- Spawning and coloring now depend on the game's `pendingVisualAreaUpdates` structure, so a game update that changes it can break the mod.
