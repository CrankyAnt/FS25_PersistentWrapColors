PWC_DEBUG = false -- set true for temporary logs

-- PWC Debug – show info if debug is set to true
local function printWrapInfo(baleObject)
    if not PWC_DEBUG then return end
    if not baleObject then return end
    local c = rawget(baleObject, "wrappingColor")
    local colorStr = (type(c) == "table" and #c >= 3)
        and string.format("R=%.3f G=%.3f B=%.3f", c[1], c[2], c[3])
        or "nil"
    print(string.format("[PWC DEBUG] Stored bale: wrapColor=%s", colorStr))
end

-- PWC_spawnMixed Spawns visuals for stored objects, each with its own wrappingColor
local function PWC_spawnMixed(objectInfo)
    local vis  = objectInfo and objectInfo.visualSpawnInfos
    local objs = objectInfo and objectInfo.objects
    if not vis or not objs or #objs == 0 then return end

    for i = 1, #vis do
        local obj = objs[((i - 1) % #objs) + 1]
        -- Spawn one by one so each visual inherits the properties (wrappingColor) of that object
        obj:spawnVisualObjects({ vis[i] })
    end
end

-- Hook in PlaceableObjectStorage
if PlaceableObjectStorage ~= nil then
    local origAdd = PlaceableObjectStorage.addObjectToObjectStorage

    ---@diagnostic disable-next-line: duplicate-set-field
    function PlaceableObjectStorage:addObjectToObjectStorage(object, loadedFromSavegame)
        if PWC_DEBUG and object and object.wrappingColor then
            printWrapInfo(object)
        end
        return origAdd(self, object, loadedFromSavegame)
    end

    ---@diagnostic disable-next-line: duplicate-set-field
    function PlaceableObjectStorage:updateObjectStorageVisualAreas()
        local spec = self.spec_objectStorage
        local area = spec.storageArea
        local oldSpawnNode = area.spawnNode

        area.spawnNode = createTransformGroup("storageAreaSpawnNode")
        link(self.rootNode, area.spawnNode)
        setVisibility(area.spawnNode, false)

        local pendingVisualAreaUpdate = {}
        pendingVisualAreaUpdate.oldSpawnNode = oldSpawnNode
        pendingVisualAreaUpdate.newSpawnNode = area.spawnNode
        pendingVisualAreaUpdate.objectInfosToSpawn = {}

        pendingVisualAreaUpdate.spawnNextObjectInfo = function()
            if #pendingVisualAreaUpdate.objectInfosToSpawn > 0 then
                local objectInfo = pendingVisualAreaUpdate.objectInfosToSpawn[1]
                PWC_spawnMixed(objectInfo)

                table.remove(pendingVisualAreaUpdate.objectInfosToSpawn, 1)

                return true
            else
                delete(pendingVisualAreaUpdate.oldSpawnNode)

                if entityExists(pendingVisualAreaUpdate.newSpawnNode) then
                    setVisibility(pendingVisualAreaUpdate.newSpawnNode, true)
                end

                return false
            end
        end

        area.spawnAreaIndex, area.spawnAreaData[1], area.spawnAreaData[2], area.spawnAreaData[3], area.spawnAreaData[4], area.spawnAreaData[5], area.spawnAreaData[6] =
            1, 0, 0, 0, 0, 0, math.huge

        for i = 1, #spec.objectInfos do
            local objectInfo = spec.objectInfos[i]
            objectInfo.visualSpawnInfos = {}
            area.spawnAreaData[6] = math.huge

            local objectToSpawn = objectInfo.objects[1]
            local ox, oy, oz, width, height, length, maxStackHeight = objectToSpawn:getSpawnInfo()
            if maxStackHeight > 1.001 then
                -- if the object is stackable, the area defines how many can be stacked on top of each other
                maxStackHeight = math.huge
            end

            for j = 1, objectInfo.numObjects do
                local areaIndex, spawnX, spawnY, spawnZ, offsetX, offsetY, offsetZ, nextOffsetX, nextOffsetZ, stackIndex =
                    PlaceableObjectStorage.getNextSpawnAreaAndOffset(area.area, area.spawnAreaIndex,
                        area.spawnAreaData[1],
                        area.spawnAreaData[2], area.spawnAreaData[3], area.spawnAreaData[4], area.spawnAreaData[5], width,
                        height, length, maxStackHeight, area.spawnAreaData[6], true)
                if areaIndex ~= nil then
                    area.spawnAreaIndex, area.spawnAreaData[1], area.spawnAreaData[2], area.spawnAreaData[3], area.spawnAreaData[4], area.spawnAreaData[5], area.spawnAreaData[6] =
                        areaIndex, offsetX, offsetY, offsetZ, nextOffsetX, nextOffsetZ, stackIndex

                    local spawnArea = area.area[area.spawnAreaIndex]

                    local cx, cy, cz = localToLocal(spawnArea.startNode, area.spawnNode, spawnX + ox, spawnY + oy,
                        spawnZ + oz)
                    local rx, ry, rz = localRotationToLocal(spawnArea.startNode, area.spawnNode, 0, 0, 0)

                    table.insert(objectInfo.visualSpawnInfos, { area.spawnNode, cx, cy, cz, rx, ry, rz })
                end
            end

            if #objectInfo.visualSpawnInfos > 0 then
                table.insert(pendingVisualAreaUpdate.objectInfosToSpawn, objectInfo)
            end
        end

        if pendingVisualAreaUpdate.spawnNextObjectInfo() then
            table.insert(spec.pendingVisualAreaUpdates, pendingVisualAreaUpdate)
            self:raiseActive()
        end
    end

    -- PWC: integritycheck, detect modconflict
    local PWC_ref_update = PlaceableObjectStorage.updateObjectStorageVisualAreas
    local PWC_ref_add    = PlaceableObjectStorage.addObjectToObjectStorage

    local PWC_Integrity  = { t = 0 }
    function PWC_Integrity:update(dt)
        self.t = self.t + (dt or 0)
        if self.t < 0.2 then return end -- short delay
        local okU = (PlaceableObjectStorage.updateObjectStorageVisualAreas == PWC_ref_update)
        local okA = (PlaceableObjectStorage.addObjectToObjectStorage == PWC_ref_add)
        if okU and okA then
            Logging.info("[PWC] active, no conflicts detected")
        else
            Logging.error(string.format("[PWC] MOD CONFLICT: patch overwritten (update=%s, add=%s)", tostring(okU),
                tostring(okA)))
        end
        removeModEventListener(PWC_Integrity)
    end

    addModEventListener(PWC_Integrity)
end
