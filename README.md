# Persistent Wrap Colors

Persistent Wrap Colors is a Farming Simulator 25 mod that keeps the original
wrap colors of wrapped bales visible while they are stored in Object Storage.

For player installation, use the official ModHub listing. This repository is the
source and distribution-policy reference.

## Download

Download Persistent Wrap Colors from the official
[GIANTS ModHub listing](https://www.farming-simulator.com/mod.php?mod_id=334607&title=fs2025).

## What This Mod Does

When wrapped bales are stored in Object Storage, this mod preserves their
individual wrap colors so they display correctly again when they are taken out.
For example, black, pink, yellow, blue, red, and green wrapped bales remain
visually distinguishable.

The mod changes bale-wrap visuals in compatible Object Storage. It does not add
storage capacity or create new Object Storage placeables. It remains lightweight
and has hardly any performance impact, including with very large numbers of bales.

## Compatibility

- Farming Simulator 25
- PC and Mac only, because this is a script mod
- Singleplayer and multiplayer
- Base-game Object Storage
- Supports modded storages that use the same object storage specialization

## Installation and Activation

1. In Farming Simulator 25, open **Downloadable Content** and install the mod
   from ModHub.
2. When creating or loading a savegame, select **Persistent Wrap Colors** in
   the mod selection.
3. Store and retrieve wrapped bales through a compatible Object Storage.

## Known Limitations

The mod relies on Object Storage behavior supplied by Farming Simulator 25.
Mods that replace the same Object Storage functions can conflict with it. When
a conflict is detected, Persistent Wrap Colors disables itself instead of
continuing with incompatible hooks.

## Reporting Issues

Report bugs and compatibility problems through the
[GitHub issue form](https://github.com/CrankyAnt/FS25_PersistentWrapColors/issues/new/choose).

Include the game version, platform, game mode, map, Object Storage involved,
bale details, relevant active mods, expected and actual behavior, and relevant
log output. Do not upload mod files.

## License and Distribution

This repository uses per-file licensing according to the REUSE specification.
See [REUSE.toml](REUSE.toml) for the authoritative machine-readable license
assignment.

The functional Lua source code is licensed under the MIT License. The official
mod name, icon, branding, descriptions, and release packages are covered by
the separate CrankyAnt Official Assets License. See
[DISTRIBUTION.md](DISTRIBUTION.md) for a human-readable explanation.

## [Changelog](CHANGELOG.md)
