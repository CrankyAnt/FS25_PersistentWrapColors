--[[ Persistent Wrap Colors v1.1.0.0
Mod for Farming Simulator 2025, keeps bale wrap colors persistent in placeable objectstorage.
server sends a compact snapshot of wrapped groups via onWriteStream; client applies
colors idempotently during visual spawn. Uses fillTypeIndex as authoritative keys.

Copyright (c) 2025 CrankyAnt

Permission is granted to download and use this mod for personal gameplay only
(including use on multiplayer servers).

Restrictions
Modification or redistribution of this mod, in whole or in part, is prohibited
without prior written permission from the author.
The only official sources for this mod are:
GitHub: https://github.com/CrankyAnt/FS25_PersistentWrapColors
GIANTS ModHub - https://www.farming-simulator.com/mod.php?mod_id=334607&title=fs2025
Any copies found outside these sources are unauthorized and may not be safe to use.

Notes
This mod is provided "as is", without warranty of any kind. Use at your own risk. ]]

-- =============================================================================
-- :: 1. CONFIGURATION & GLOBAL VARIABLES ::
-- =============================================================================

---@class PersistentWrapColors
PersistentWrapColors = {}
PersistentWrapColors.MOD_NAME = "[PWC]"
PersistentWrapColors.DEBUG = false

-- Per-placeable/storage mapping
PersistentWrapColors.serverColorDB = {} -- [placeable] = { displayId:string, groups: table<string,{seq: { {r,g,b}... }, count:number}>, totals = {bales=number, wrapped=number}}
PersistentWrapColors.clientColorDB = {} -- [placeable] = { groups: table<string,{seq: { {r,g,b}... }, pos:number}> }
PersistentWrapColors.ENABLED = true
PersistentWrapColors._patchedRefs = {}  -- Store patched function references

-- Forward declaration
local pwc_PlaceableObjectStorage_updateObjectStorageVisualAreas

-- Store original basegame references for later comparison
PersistentWrapColors._basegameRefs = {
    loadFromXMLFile = PlaceableObjectStorage and PlaceableObjectStorage.loadFromXMLFile,
    addObjectToObjectStorage = PlaceableObjectStorage and PlaceableObjectStorage.addObjectToObjectStorage,
    addAbstactObjectToObjectStorage = PlaceableObjectStorage and
        PlaceableObjectStorage.addAbstactObjectToObjectStorage,
    removeAbstractObjectsFromStorage = PlaceableObjectStorage and
        PlaceableObjectStorage.removeAbstractObjectsFromStorage,
    onWriteStream = PlaceableObjectStorage and PlaceableObjectStorage.onWriteStream,
    onReadStream = PlaceableObjectStorage and PlaceableObjectStorage.onReadStream,
    updateObjectStorageVisualAreas = PlaceableObjectStorage and PlaceableObjectStorage
        .updateObjectStorageVisualAreas
}

-- =============================================================================
-- :: 2. HELPER FUNCTIONS ::
-- =============================================================================
-- Generic utilities (logging, clamping, safe calls, filename normalization),
-- key generation for group entries, and the server 'expect update' marker.
local function pwc_logf(fmt, ...)
    if PersistentWrapColors.DEBUG then
        print(PersistentWrapColors.MOD_NAME .. " " .. string.format(fmt, ...))
    end
end

local function clamp255(x)
    return math.max(0, math.min(255, math.floor(((x or 0.85) * 255) + 0.5)))
end

local function pwc_safeCallMethod(obj, methodName)
    local f = obj and obj[methodName]
    if type(f) == "function" then
        local ok, res = pcall(f, obj)
        if ok then return res end
    end
    return nil
end

local function normalizeFilename(path)
    if path == nil then return nil end
    if NetworkUtil and NetworkUtil.convertToNetworkFilename then
        return NetworkUtil.convertToNetworkFilename(path)
    end
    return path
end

-- Use fillTypeIndex as a stable key (GIANTS index is authoritative, including modded types).
local function getFillTypeKey(ft)
    if ft == nil then return -1 end
    if type(ft) == "number" then
        return ft
    end
    if type(ft) == "string" then
        if g_fillTypeManager and g_fillTypeManager.getFillTypeIndexByName then
            local idx = g_fillTypeManager:getFillTypeIndexByName(ft)
            if idx ~= nil then return idx end
        end
        return -1
    end
    return -1
end

local function makeGroupKey(className, filename, fillTypeKey, variationIndex)
    return string.format("%s|%s|%s|%s", className or "?", filename or "?", tostring(fillTypeKey or -1),
        tostring(variationIndex or 1))
end

local function pwc_expectUpdate(self)
    local spec = self.spec_objectStorage
    if spec ~= nil then
        spec._pwcExpectUpdate = true
    end
end

-- SP helper: copy server snapshot into client DB once per storage (singleplayer only)
local function pwc_sp_seedClientFromServer(self)
    if not (g_currentMission and g_currentMission:getIsClient() and g_currentMission:getIsServer()) then return end
    local key = self.owningPlaceable or self
    if PersistentWrapColors.clientColorDB[key] then return end

    local store = PersistentWrapColors.serverColorDB[key]
    local groups = {}
    if store and store.groups then
        for k, data in pairs(store.groups) do
            local seq = {}
            if data.seq then
                for i = 1, #data.seq do
                    local c = data.seq[i]
                    seq[i] = { c[1], c[2], c[3] }
                end
            end
            groups[k] = { seq = seq, pos = 0 }
        end
    end

    PersistentWrapColors.clientColorDB[key] = { groups = groups }
    local n = 0; for _ in pairs(groups) do n = n + 1 end
    pwc_logf("SP-SEED storage=%s groups=%d (seeded from server)", tostring(key), n)
end

-- SP helper: derive exact group key (and wrapping state) for an objectInfo via storedObjects
local function pwc_resolveKeyViaStoredObjects(self, objectInfo)
    local spec = self.spec_objectStorage
    if not (spec and spec.objectInfos and spec.storedObjects) then return nil, nil end

    -- Find the index of the objectInfo in spec.objectInfos
    local oiIdx = nil
    for i = 1, #spec.objectInfos do
        if spec.objectInfos[i] == objectInfo then
            oiIdx = i; break
        end
    end
    if oiIdx == nil then return nil, nil end

    -- Look up the matching storedObject (has bale attributes used by the server DB)
    for j = 1, #spec.storedObjects do
        local so = spec.storedObjects[j]
        if so and so.objectInfoIndex == oiIdx then
            local bo = so.baleObject or so.bale or so.object
            local ba = so.baleAttributes or (bo and bo.baleAttributes) or bo
            if ba then
                local filename = normalizeFilename(ba.xmlFilename or ba.configFileName)
                local ftKey    = getFillTypeKey(ba.fillType or ba.fillTypeIndex)
                local ws       = ba.wrappingState or (bo and (pwc_safeCallMethod(bo, "getWrappingState") or 0)) or 0
                if filename and ftKey and ftKey ~= -1 then
                    return makeGroupKey("Bale", filename, ftKey, 1), ws
                end
            end
        end
    end
    return nil, nil
end

-- =============================================================================
-- :: 3. CORE LOGIC (SERVER-SIDE) ::
-- =============================================================================
-- Server DB helpers
-- =============================================================================
local function pwc_srv_getStore(self)
    local placeableKey = self.owningPlaceable or self
    local store = PersistentWrapColors.serverColorDB[placeableKey]
    if store == nil then
        store = { displayId = tostring(placeableKey), groups = {}, totals = { bales = 0, wrapped = 0 } }
        PersistentWrapColors.serverColorDB[placeableKey] = store
    end
    return store
end

-- Idempotent rebuild: compute groups + counts + colors purely from storedObjects (wrapped>0).
-- totals.bales is optionally derived from objectInfos for debugging.
local function pwc_srv_rebuildFromStorageState(self)
    if g_currentMission == nil or not g_currentMission:getIsServer() then return end

    local spec = self.spec_objectStorage
    local store = pwc_srv_getStore(self)

    local newGroups = {}
    local totalsBales, totalsWrapped = 0, 0

    if spec == nil then
        pwc_logf("REBUILD storage=%s (no spec)", tostring(self.owningPlaceable or self))
        return
    end

    -- (A) Debug only: total #bales from objectInfos
    if spec.objectInfos ~= nil then
        for i = 1, #spec.objectInfos do
            local info = spec.objectInfos[i]
            totalsBales = totalsBales + ((info and info.numObjects) or 0)
        end
    end

    -- (B) Groups + colors + counts entirely from storedObjects (wrapped>0)
    if spec.storedObjects ~= nil and #spec.storedObjects > 0 then
        for i = 1, #spec.storedObjects do
            local so = spec.storedObjects[i]
            local baleObj = so and (so.baleObject or so.bale or so.object)
            local ba = nil
            if baleObj ~= nil then
                ba = baleObj
            elseif so and so.baleAttributes ~= nil then
                ba = so.baleAttributes
            end

            if ba ~= nil then
                local ws = ba.wrappingState or (baleObj and (pwc_safeCallMethod(baleObj, "getWrappingState") or 0)) or 0
                if ws and ws > 0.5 then
                    local filename = normalizeFilename(ba.xmlFilename or ba.configFileName)
                    local ftKey    = getFillTypeKey(ba.fillType or ba.fillTypeIndex)
                    local key      = makeGroupKey("Bale", filename, ftKey, 1)

                    local gdata    = newGroups[key]
                    if gdata == nil then
                        gdata = { seq = {} }
                        newGroups[key] = gdata
                    end

                    local col = ba.wrappingColor or ba.wrapColor
                    local r, g, b = 0.85, 0.85, 0.85
                    if type(col) == "table" then
                        r = col[1] or r; g = col[2] or g; b = col[3] or b
                    end
                    table.insert(gdata.seq, { r, g, b })
                    pwc_logf("SRV ADD key=%s col=%.3f %.3f %.3f", key, r, g, b)
                end
            end
        end
    end

    -- (C) Finalize counts from seq length and totals.wrapped
    for key, g in pairs(newGroups) do
        g.count = (g.seq and #g.seq) or 0
        totalsWrapped = totalsWrapped + g.count
    end

    store.groups = newGroups
    store.totals = { bales = totalsBales, wrapped = totalsWrapped }

    for key, g in pairs(store.groups) do
        pwc_logf("SRV SEQVSCOUNT key=%s seq=%d count=%d", key, (g.seq and #g.seq) or 0, g.count or 0)
    end
    local gcount = 0; for _ in pairs(store.groups) do gcount = gcount + 1 end
    pwc_logf("REBUILD storage=%s groups=%d bales=%d wrapped=%d", store.displayId, gcount, totalsBales, totalsWrapped)
end

local function pwc_markDirty(self)
    if g_currentMission and g_currentMission:getIsServer() then
        local spec = self.spec_objectStorage
        if spec and spec.dirtyFlag then
            spec._pwcExpectUpdate = true
            self:raiseDirtyFlags(spec.dirtyFlag)
            pwc_logf("DIRTY storage=%s", tostring(self.owningPlaceable or self))
        end
    end
end

-- =============================================================================
-- Server hooks
-- =============================================================================
local function pwc_PlaceableObjectStorage_loadFromXMLFile(self, superFunc, xmlFile, key)
    local ret = superFunc(self, xmlFile, key)
    if g_currentMission == nil or not g_currentMission:getIsServer() then return ret end

    -- optional: read a display-id from parent <placeable#uniqueId>
    local parentKey = key
    local m = parentKey and parentKey:match("(.+)%.objectStorage$")
    if m ~= nil then parentKey = m end
    local displayId = xmlFile:getValue((parentKey or "") .. "#uniqueId") or parentKey or "<no-id>"

    local store = pwc_srv_getStore(self)
    store.displayId = store.displayId or displayId

    -- After load: build snapshot from saved state
    pwc_srv_rebuildFromStorageState(self)

    local spec = self.spec_objectStorage
    if spec then spec._pwcRebuilt = true end

    return ret
end

local function pwc_PlaceableObjectStorage_addObjectToObjectStorage(self, superFunc, object, loadedFromSavegame)
    -- Pre-base: expect UPDATE only when not loading from savegame
    if g_currentMission and g_currentMission:getIsServer() and not loadedFromSavegame then
        pwc_expectUpdate(self)
    end

    -- Base call
    local ret = superFunc(self, object, loadedFromSavegame)

    -- Post-base: rebuild; markDirty only if this was not a savegame load
    if g_currentMission and g_currentMission:getIsServer() then
        pwc_srv_rebuildFromStorageState(self)

        local spec = self.spec_objectStorage
        if spec then spec._pwcRebuilt = true end

        if not loadedFromSavegame then
            pwc_logf("AFTER ADD: calling markDirty (loadedFromSavegame=%s)", tostring(loadedFromSavegame))
            pwc_markDirty(self)
        else
            pwc_logf("AFTER ADD: skip markDirty for savegame load")
        end
    end

    return ret
end

local function pwc_PlaceableObjectStorage_addAbstactObjectToObjectStorage(self, superFunc, objectInfoIndex, amount,
                                                                          connection)
    pwc_expectUpdate(self)
    local ret = superFunc(self, objectInfoIndex, amount, connection)
    if g_currentMission and g_currentMission:getIsServer() then
        pwc_srv_rebuildFromStorageState(self)
        local spec = self.spec_objectStorage
        if spec then spec._pwcRebuilt = true end

        pwc_logf("AFTER ADD_ABSTRACT: calling markDirty (idx=%s, amount=%s)", tostring(objectInfoIndex),
            tostring(amount))
        pwc_markDirty(self)
    end
    return ret
end

local function pwc_PlaceableObjectStorage_removeAbstractObjectsFromStorage(self, superFunc, objectInfoIndex, amount,
                                                                           connection)
    pwc_expectUpdate(self)
    local ret = superFunc(self, objectInfoIndex, amount, connection)
    if g_currentMission and g_currentMission:getIsServer() then
        pwc_srv_rebuildFromStorageState(self)
        local spec = self.spec_objectStorage
        if spec then spec._pwcRebuilt = true end

        pwc_logf("AFTER REMOVE ABSTRACTS: calling markDirty (idx=%s, amount=%s)", tostring(objectInfoIndex),
            tostring(amount))
        pwc_markDirty(self)
    end

    return ret
end

local function pwc_PlaceableObjectStorage_onWriteStream(self, superFunc, streamId, connection)
    if g_currentMission and g_currentMission:getIsServer() then
        if connection ~= nil and connection.getIsServer ~= nil and connection:getIsServer() then
            return superFunc(self, streamId, connection)
        end

        local path = "INITIAL"
        local spec = self.spec_objectStorage
        local skipRebuild = (spec and spec._pwcRebuilt) or false
        if skipRebuild then
            pwc_logf("SKIP REBUILD (already rebuilt pre-dirty)")
        else
            pwc_srv_rebuildFromStorageState(self)
        end
        if spec then spec._pwcRebuilt = false end
        -- when dirty is set, we expect an update
        if spec and spec._pwcExpectUpdate == true then
            path = "UPDATE"
            spec._pwcExpectUpdate = false
        end
        pwc_logf("PATH WRITE: %s", path)

        local placeableKey = self.owningPlaceable or self
        local store = PersistentWrapColors.serverColorDB[placeableKey]
        local groups = (store and store.groups) or {}

        -- Send all wrapped groups (even if seq is empty)
        local gcount = 0; for _ in pairs(groups) do gcount = gcount + 1 end
        streamWriteUInt16(streamId, gcount)
        for key, data in pairs(groups) do
            local seq = (data and data.seq) or {}
            streamWriteString(streamId, key)
            streamWriteUInt16(streamId, #seq)
            for i = 1, #seq do
                local c = seq[i]
                streamWriteUInt8(streamId, clamp255(c[1]))
                streamWriteUInt8(streamId, clamp255(c[2]))
                streamWriteUInt8(streamId, clamp255(c[3]))
            end
        end
        pwc_logf("ONWRITESTREAM storage=%s groups=%d (pre-base)", store and store.displayId or tostring(self), gcount)
    end
    superFunc(self, streamId, connection)
end

-- =============================================================================
-- :: 4. MULTIPLAYER EVENT HANDLER (CLIENT-SIDE) ::
-- =============================================================================
-- Client networking: receive snapshot before visuals; hook visuals per instance.
local function pwc_PlaceableObjectStorage_onReadStream(self, superFunc, streamId, connection)
    if g_currentMission and g_currentMission:getIsClient() then
        local gcount = streamReadUInt16(streamId)
        local newGroups = {}
        for _ = 1, gcount do
            local key = streamReadString(streamId)
            local n = streamReadUInt16(streamId)
            local seq = {}
            for i = 1, n do
                local r = streamReadUInt8(streamId) / 255
                local g = streamReadUInt8(streamId) / 255
                local b = streamReadUInt8(streamId) / 255
                seq[i] = { r, g, b }
            end
            newGroups[key] = { seq = seq, pos = 0 }
        end

        local storageKey = self.owningPlaceable or self
        PersistentWrapColors.clientColorDB[storageKey] = { groups = newGroups }
        pwc_logf("ONREADSTREAM storage=%s groups=%d (pre-base)", tostring(storageKey), gcount)

        -- Visual patching already done at script load time with Utils.overwrittenFunction
    end

    -- Call base
    superFunc(self, streamId, connection)
end

-- =============================================================================
-- :: 5. CLIENT-SIDE VISUAL SPAWNING & COLORING ::
-- =============================================================================
-- Client: apply color to newly spawned visuals
local function pwc_applyColorToNodeDeep(node, r, g, b)
    if getHasShaderParameter(node, "colorScale") then
        setShaderParameter(node, "colorScale", r, g, b, 1, false)
    end
    for i = 0, getNumOfChildren(node) - 1 do
        pwc_applyColorToNodeDeep(getChildAt(node, i), r, g, b)
    end
end

local function pwc_extractRepInfo(objectInfo)
    local rep = objectInfo and objectInfo.objects and objectInfo.objects[1]
    if rep == nil then return nil end
    local ba = rep.baleAttributes or rep
    local filename = normalizeFilename(ba.xmlFilename or ba.configFileName or "?")
    local ftKey = getFillTypeKey(ba.fillType or ba.fillTypeIndex)
    local wrappingState = (ba.wrappingState ~= nil) and ba.wrappingState or 0
    return filename, ftKey, wrappingState
end

local function pwc_colorizeLastSpawnBatch(self, objectInfo, parentNode, beforeChildCount)
    local storageKey = self.owningPlaceable or self
    local idxDB = PersistentWrapColors.clientColorDB[storageKey]
    if not (idxDB and idxDB.groups) then
        pwc_logf("SPAWN NODB storage=%s", tostring(storageKey))
        return
    end

    local filename, ftKey, ws = pwc_extractRepInfo(objectInfo)
    pwc_logf("REPDUMP file=%s ftIdx=%s wrap=%s", tostring(filename or "?"), tostring(ftKey or -1), tostring(ws or 0))

    if (ws or 0) < 1 then
        pwc_logf("SKIP storage=%s notWrapped ws=%s", tostring(storageKey), tostring(ws))
        return
    end

    local key = makeGroupKey("Bale", filename, ftKey, 1)
    local group = idxDB.groups[key]
    if group == nil then
        -- Log known keys for comparison
        local known = {}
        for k, _ in pairs(idxDB.groups) do table.insert(known, k) end
        pwc_logf("MISS groupKey=%s knownKeys=%s", key, table.concat(known, ","))
        return
    end

    local after = getNumOfChildren(parentNode)
    local startIdx = beforeChildCount or 0
    local newCount = math.max(0, after - startIdx)
    pwc_logf("COLORSEQ key=%s newVisuals=%d pos=%d len=%d",
        key, newCount, group.pos or 0, (group.seq and #group.seq) or 0)
    local used = 0

    for i = 0, newCount - 1 do
        group.pos = (group.pos or 0) + 1
        local c = group.seq[group.pos]
        local r, g, b = 0.85, 0.85, 0.85
        if c ~= nil then r, g, b = c[1] or r, c[2] or g, c[3] or b end
        local node = getChildAt(parentNode, startIdx + i)
        if node ~= nil then pwc_applyColorToNodeDeep(node, r, g, b) end
        used = used + 1
    end

    pwc_logf("COLORDBG storage=%s key=%s vis=%d pos=%d/%d", tostring(storageKey), key, newCount, group.pos or 0,
        (group.seq and #group.seq) or 0)
    pwc_logf("COLOR group=%s used=%d pos=%d/%d", key, used, group.pos or 0, (group.seq and #group.seq) or 0)
end

-- PWC_spawnMixed Spawns visuals for stored objects, each with its own wrappingColor
local function PWC_spawnMixed(objectInfo)
    local vis  = objectInfo and objectInfo.visualSpawnInfos
    local objs = objectInfo and objectInfo.objects
    if not vis or not objs or #objs == 0 then
        pwc_logf("PWC_spawnMixed: No vis or objs")
        return
    end

    pwc_logf("PWC_spawnMixed: Spawning %d visuals with %d objects", #vis, #objs)

    for i = 1, #vis do
        local obj = objs[((i - 1) % #objs) + 1]
        -- Spawn one by one so each visual inherits the properties (wrappingColor) of that object
        obj:spawnVisualObjects({ vis[i] })

        -- Debug: check if object has wrappingColor
        if PersistentWrapColors.DEBUG then
            local c = rawget(obj, "wrappingColor")
            if c then
                pwc_logf("  Visual %d: color=%.3f,%.3f,%.3f", i, c[1], c[2], c[3])
            else
                pwc_logf("  Visual %d: NO COLOR on object", i)
            end
        end
    end
end

-- =============================================================================
-- :: 6. VISUALS OVERRIDE (SPAWN → COLORIZE → HANDOVER) ::
-- =============================================================================
pwc_PlaceableObjectStorage_updateObjectStorageVisualAreas = function(self, superFunc)
    local spec = self.spec_objectStorage
    local area = spec and spec.storageArea

    -- Check if we're in singleplayer
    local isSingleplayer = g_currentMission and g_currentMission:getIsClient() and g_currentMission:getIsServer()

    pwc_logf("VISUALS START storage=%s infos=%d SP=%s", tostring(self.owningPlaceable or self),
        (spec and spec.objectInfos and #spec.objectInfos) or -1, tostring(isSingleplayer))

    -- Reset group positions for MP when doing any visual update
    if not isSingleplayer and g_currentMission and g_currentMission:getIsClient() then
        local storageKey = self.owningPlaceable or self
        local clientDB = PersistentWrapColors.clientColorDB[storageKey]
        if clientDB and clientDB.groups then
            -- Always reset group positions when visuals are being updated
            -- This ensures correct positioning whether adding new bales or rebuilding storage (for edge cases)
            for key, group in pairs(clientDB.groups) do
                group.pos = 0
                pwc_logf("MP: Reset group.pos for key=%s", key)
            end
        end
    end

    if isSingleplayer then
        -- SP: Need to handle first spawn differently
        -- Store original objects to respawn with colors
        local firstObjectInfo = nil
        local firstVisualSpawnInfos = nil

        -- Call superFunc to let basegame/other mods work
        superFunc(self)

        -- Hook pending updates and fix the first spawn
        if spec and spec.pendingVisualAreaUpdates and #spec.pendingVisualAreaUpdates > 0 then
            local pending = spec.pendingVisualAreaUpdates[#spec.pendingVisualAreaUpdates]

            -- Check if first group was already spawned (it usually is)
            local totalObjectInfos = #spec.objectInfos
            local remainingToSpawn = #pending.objectInfosToSpawn

            if remainingToSpawn < totalObjectInfos then
                -- First was already spawned, we need to fix it
                pwc_logf("SP: First group already spawned, need to re-spawn with colors")

                -- Find and remove the already-spawned visuals
                firstObjectInfo = spec.objectInfos[1]
                if firstObjectInfo and firstObjectInfo.visualSpawnInfos then
                    firstVisualSpawnInfos = firstObjectInfo.visualSpawnInfos
                    local parentNode = firstVisualSpawnInfos[1] and firstVisualSpawnInfos[1][1] or area.spawnNode

                    -- Delete the non-colored visuals that were just spawned
                    local numChildren = getNumOfChildren(parentNode)
                    local numToDelete = math.min(#firstVisualSpawnInfos, numChildren)
                    pwc_logf("SP: Deleting %d visuals (parentNode has %d children)", numToDelete, numChildren)
                    for i = numToDelete - 1, 0, -1 do
                        if i < numChildren then
                            local child = getChildAt(parentNode, i)
                            if child and entityExists(child) then
                                delete(child)
                            end
                        end
                    end

                    -- Re-insert first objectInfo at beginning of spawn queue
                    table.insert(pending.objectInfosToSpawn, 1, firstObjectInfo)
                end
            end

            -- Now hook the spawn function to use PWC_spawnMixed
            local originalSpawnNext = pending.spawnNextObjectInfo
            pending.spawnNextObjectInfo = function()
                if #pending.objectInfosToSpawn > 0 then
                    local objectInfo = pending.objectInfosToSpawn[1]
                    pwc_logf("SP spawnNextObjectInfo: colored spawn for %d visuals",
                        #(objectInfo.visualSpawnInfos or {}))
                    PWC_spawnMixed(objectInfo)
                    table.remove(pending.objectInfosToSpawn, 1)
                    return true
                else
                    -- Let original handle cleanup
                    return originalSpawnNext()
                end
            end
        end

        pwc_logf("SP MODE - fixed first spawn and hooked for colored spawning")
    else
        -- MP: Different approach - colorize after spawn
        superFunc(self)

        if spec and spec.pendingVisualAreaUpdates and #spec.pendingVisualAreaUpdates > 0 then
            local pending = spec.pendingVisualAreaUpdates[#spec.pendingVisualAreaUpdates]

            -- Check if first group was already spawned (same as SP)
            local totalObjectInfos = #spec.objectInfos
            local remainingToSpawn = #pending.objectInfosToSpawn

            if remainingToSpawn < totalObjectInfos then
                -- First was already spawned, apply colors retroactively
                pwc_logf("MP: First group already spawned, applying retroactive coloring")

                local firstObjectInfo = spec.objectInfos[1]
                if firstObjectInfo and firstObjectInfo.visualSpawnInfos and #firstObjectInfo.visualSpawnInfos > 0 then
                    local parentNode = firstObjectInfo.visualSpawnInfos[1][1] or area.spawnNode
                    -- The visuals are already spawned as first N children
                    pwc_colorizeLastSpawnBatch(self, firstObjectInfo, parentNode, 0)
                end
            end

            -- Hook for remaining spawns
            local originalSpawnNext = pending.spawnNextObjectInfo
            pending.spawnNextObjectInfo = function()
                local objectInfoToColor = pending.objectInfosToSpawn[1]
                local parentNode = nil
                local beforeCount = 0

                if objectInfoToColor and objectInfoToColor.visualSpawnInfos and #objectInfoToColor.visualSpawnInfos > 0 then
                    parentNode = objectInfoToColor.visualSpawnInfos[1][1] or area.spawnNode
                    beforeCount = getNumOfChildren(parentNode)
                end

                local result = originalSpawnNext()

                if result and objectInfoToColor and g_currentMission and g_currentMission:getIsClient() then
                    pwc_logf("MP: Applying colors after spawn")
                    pwc_colorizeLastSpawnBatch(self, objectInfoToColor, parentNode, beforeCount)
                end

                return result
            end
        end

        pwc_logf("MP MODE - hooked for post-spawn coloring with first-group fix")
    end
end

-- =============================================================================
-- :: 7. SINGLEPLAYER DIRECT REPLACEMENT (AT SCRIPT LOAD) ::
-- =============================================================================
-- Use Utils.overwrittenFunction for proper superFunc compatibility
if PlaceableObjectStorage ~= nil then
    PlaceableObjectStorage.updateObjectStorageVisualAreas = Utils.overwrittenFunction(
        PlaceableObjectStorage.updateObjectStorageVisualAreas,
        pwc_PlaceableObjectStorage_updateObjectStorageVisualAreas
    )

    PersistentWrapColors._patchedRefs.updateObjectStorageVisualAreas = PlaceableObjectStorage
        .updateObjectStorageVisualAreas
    pwc_logf("PlaceableObjectStorage.updateObjectStorageVisualAreas PATCHED with Utils.overwrittenFunction")
end

-- =============================================================================
-- :: 8. INITIALIZATION & INTEGRITY CHECK ::
-- =============================================================================
-- Check immediately after patching if patches were applied correctly
local function postPatchIntegrityCheck()
    local conflicts = {}

    -- Verify patches are in place
    for name, expectedRef in pairs(PersistentWrapColors._patchedRefs or {}) do
        local currentRef = PlaceableObjectStorage and PlaceableObjectStorage[name]
        if currentRef ~= expectedRef then
            local origin = "unknown"
            if debug and debug.getinfo then
                local info = debug.getinfo(currentRef)
                if info and info.source then
                    origin = info.source
                end
            end
            table.insert(conflicts, string.format("%s from %s", name, origin))
        end
    end

    return #conflicts == 0, conflicts
end

-- Run post-patch check immediately
local patchSuccess, patchConflicts = postPatchIntegrityCheck()
if not patchSuccess then
    PersistentWrapColors.ENABLED = false
    print(string.format("[PWC] POST-PATCH CONFLICT DETECTED by: %s - disabling PersistentWrapColors",
        table.concat(patchConflicts, "; ")))
    return
else
    print("[PWC] No conflict detected - Persistent WrapColors enabled")
    -- Schedule runtime check for later
    PersistentWrapColors._integrityCheckPending = true
end

-- Install server and client hooks during map load; patch logs confirm activation.
function PersistentWrapColors:loadMap()
    -- Server hooks
    if g_currentMission and g_currentMission:getIsServer() then
        if PlaceableObjectStorage and PlaceableObjectStorage.loadFromXMLFile and not self._hooked_loadFromXMLFile then
            PlaceableObjectStorage.loadFromXMLFile = Utils.overwrittenFunction(PlaceableObjectStorage.loadFromXMLFile,
                pwc_PlaceableObjectStorage_loadFromXMLFile)
            PersistentWrapColors._patchedRefs.loadFromXMLFile = PlaceableObjectStorage.loadFromXMLFile
            self._hooked_loadFromXMLFile = true
            pwc_logf("PATCHED PlaceableObjectStorage:loadFromXMLFile (server-only)")
        end
        if PlaceableObjectStorage and PlaceableObjectStorage.addObjectToObjectStorage and not self._hooked_addObj then
            PlaceableObjectStorage.addObjectToObjectStorage = Utils.overwrittenFunction(
                PlaceableObjectStorage.addObjectToObjectStorage, pwc_PlaceableObjectStorage_addObjectToObjectStorage)
            PersistentWrapColors._patchedRefs.addObjectToObjectStorage = PlaceableObjectStorage.addObjectToObjectStorage
            self._hooked_addObj = true
            pwc_logf("PATCHED PlaceableObjectStorage:addObjectToObjectStorage (server-only)")
        end
        if PlaceableObjectStorage and PlaceableObjectStorage.addAbstactObjectToObjectStorage and not self._hooked_addAbs then
            PlaceableObjectStorage.addAbstactObjectToObjectStorage =
                Utils.overwrittenFunction(
                    PlaceableObjectStorage.addAbstactObjectToObjectStorage,
                    pwc_PlaceableObjectStorage_addAbstactObjectToObjectStorage
                )
            PersistentWrapColors._patchedRefs.addAbstactObjectToObjectStorage = PlaceableObjectStorage
                .addAbstactObjectToObjectStorage
            self._hooked_addAbs = true
            pwc_logf("PATCHED PlaceableObjectStorage:addAbstactObjectToObjectStorage (server-only)")
        end
        if PlaceableObjectStorage and PlaceableObjectStorage.removeAbstractObjectsFromStorage and not self._hooked_removeAbs then
            PlaceableObjectStorage.removeAbstractObjectsFromStorage = Utils.overwrittenFunction(
                PlaceableObjectStorage.removeAbstractObjectsFromStorage,
                pwc_PlaceableObjectStorage_removeAbstractObjectsFromStorage)
            PersistentWrapColors._patchedRefs.removeAbstractObjectsFromStorage = PlaceableObjectStorage
                .removeAbstractObjectsFromStorage
            self._hooked_removeAbs = true
            pwc_logf("PATCHED PlaceableObjectStorage:removeAbstractObjectsFromStorage (server-only)")
        end
        if PlaceableObjectStorage and PlaceableObjectStorage.onWriteStream and not self._hooked_writeStream then
            PlaceableObjectStorage.onWriteStream = Utils.overwrittenFunction(PlaceableObjectStorage.onWriteStream,
                pwc_PlaceableObjectStorage_onWriteStream)
            PersistentWrapColors._patchedRefs.onWriteStream = PlaceableObjectStorage.onWriteStream
            self._hooked_writeStream = true
            pwc_logf("PATCHED PlaceableObjectStorage:onWriteStream (server-only)")
        end
    end

    -- Client hooks
    if g_currentMission and g_currentMission:getIsClient() then
        if PlaceableObjectStorage and PlaceableObjectStorage.onReadStream and not self._hooked_readStream then
            PlaceableObjectStorage.onReadStream = Utils.overwrittenFunction(PlaceableObjectStorage.onReadStream,
                pwc_PlaceableObjectStorage_onReadStream)
            PersistentWrapColors._patchedRefs.onReadStream = PlaceableObjectStorage.onReadStream
            self._hooked_readStream = true
            pwc_logf("PATCHED PlaceableObjectStorage:onReadStream (client-only)")
        end
    end
end

-- Run a final integrity check to catch any runtime conflicts
function PersistentWrapColors:_runIntegrityCheck()
    self._integrityCheckPending = false
    local conflicts = {}

    local function check(name, current)
        local ours = self._patchedRefs and self._patchedRefs[name]
        if ours and current ~= ours then
            local origin = "unknown"
            if debug and debug.getinfo then
                local info = debug.getinfo(current)
                if info and info.source then
                    origin = info.source
                end
            end
            table.insert(conflicts, string.format("%s from %s", name, origin))
        end
    end

    if PlaceableObjectStorage then
        check("loadFromXMLFile", PlaceableObjectStorage.loadFromXMLFile)
        check("addObjectToObjectStorage", PlaceableObjectStorage.addObjectToObjectStorage)
        check("addAbstactObjectToObjectStorage", PlaceableObjectStorage.addAbstactObjectToObjectStorage)
        check("removeAbstractObjectsFromStorage", PlaceableObjectStorage.removeAbstractObjectsFromStorage)
        check("onWriteStream", PlaceableObjectStorage.onWriteStream)
        check("onReadStream", PlaceableObjectStorage.onReadStream)
        check("updateObjectStorageVisualAreas", PlaceableObjectStorage.updateObjectStorageVisualAreas)
    end

    if #conflicts > 0 then
        PersistentWrapColors.ENABLED = false
        print(string.format("[PWC] RUNTIME CONFLICT DETECTED by: %s - disabling PersistentWrapColors",
            table.concat(conflicts, "; ")))
    end
end

function PersistentWrapColors:update(dt)
    if self._integrityCheckPending then
        self:_runIntegrityCheck()
    end
end

addModEventListener(PersistentWrapColors)
