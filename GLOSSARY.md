# Persistent Wrap Colors

Persistent Wrap Colors keeps the wrapping color of each wrapped bale visible while the bale is stored in an Object Storage, in singleplayer and multiplayer.

## Language

### Game concepts

**Object Storage**:
A placeable that stores objects such as bales without keeping them as physical objects in the world, and shows them as visuals in its storage area.
_Avoid_: warehouse, bale storage, storage mod

**Stored object**:
The game's record of one bale held in an Object Storage, carrying that bale's attributes, including its wrapping state and wrapping color.
_Avoid_: stored bale, bale entry

**Object info**:
The game's aggregate of identical objects in one Object Storage, with their number and the visuals that represent them.
_Avoid_: stack, object group

**Visual**:
The display model that an Object Storage spawns in its storage area to represent one stored bale.
_Avoid_: bale model, dummy bale

**Visual area update**:
The game's pass in which an Object Storage spawns the visuals for all its object infos again, one object info at a time.
_Avoid_: refresh, redraw

**Wrapping state**:
How far a bale is wrapped, from 0 (unwrapped) to 1 (fully wrapped).
_Avoid_: wrap progress

**Wrapping color**:
The color of the film around a wrapped bale.
_Avoid_: wrap color, foil color

**Fill type**:
The material a bale contains, such as grass or silage, including fill types that other mods add.
_Avoid_: content, material type

### Mod concepts

**Wrapped bale**:
A bale whose wrapping state shows it is wrapped; only wrapped bales have a wrapping color that the mod keeps.
_Avoid_: silage bale, colored bale

**Group**:
All wrapped bales in one Object Storage that share the same bale XML file and fill type.
_Avoid_: bale type, category

**Group key**:
The text that identifies a group within one Object Storage, built from the bale XML file and the fill type.
_Avoid_: group ID, bale key

**Color sequence**:
The ordered list of wrapping colors of the wrapped bales in one group, one color per bale.
_Avoid_: palette, color list

**Position**:
How many colors of a color sequence a client has already applied to visuals during the current visual area update.
_Avoid_: cursor, index, offset

**Server color DB**:
The server's record of the groups and color sequences of every Object Storage; the source of truth for wrapping colors.
_Avoid_: snapshot, store

**Client color DB**:
A client's copy of the groups and color sequences of every Object Storage, as received from the server, with a position per group.
_Avoid_: snapshot, cache

**Rebuild**:
Building the server color DB entry of one Object Storage again from its stored objects.
_Avoid_: refresh, resync

**Patch**:
A replacement of an Object Storage game function that runs the mod's logic and calls the replaced function.
_Avoid_: hook, override

**Integrity check**:
The check that every patch is still the active version of its game function, once when the script loads and once on the first update.
_Avoid_: compatibility check, self-test

**Conflict**:
A patched function that another mod replaced after this mod patched it.
_Avoid_: incompatibility, clash
