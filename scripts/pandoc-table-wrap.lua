-- Pandoc Lua filter: fix table rendering for PDF output.
--   1. Redistribute column widths proportionally based on content length.
--   2. Insert zero-width spaces at camelCase/underscore boundaries so
--      long identifiers can line-break inside narrow columns.

local ZWSP = "\xE2\x80\x8B"
local MIN_COL_WIDTH = 0.08

local function add_word_breaks(text)
  text = text:gsub("(%l)(%u)", "%1" .. ZWSP .. "%2")
  text = text:gsub("(%u%u)(%u%l)", "%1" .. ZWSP .. "%2")
  text = text:gsub("(_)(%w)", "%1" .. ZWSP .. "%2")
  text = text:gsub("(%w)(%.%u)", "%1" .. ZWSP .. "%2")
  return text
end

local break_filter = {
  Code = function(el)
    local t = add_word_breaks(el.text)
    if t ~= el.text then
      el.text = t
      return el
    end
  end,
  Str = function(el)
    if #el.text > 12 then
      local t = add_word_breaks(el.text)
      if t ~= el.text then
        el.text = t
        return el
      end
    end
  end
}

local function process_rows(rows)
  for _, row in ipairs(rows) do
    for _, cell in ipairs(row.cells) do
      local new_contents = pandoc.List()
      for _, block in ipairs(cell.contents) do
        new_contents:insert(pandoc.walk_block(block, break_filter))
      end
      cell.contents = new_contents
    end
  end
end

local function cell_text_length(cell)
  if cell.contents then
    return #pandoc.utils.stringify(cell.contents)
  end
  return #pandoc.utils.stringify(cell)
end

function Table(tbl)
  local ncols = #tbl.colspecs
  if ncols == 0 then return end

  local max_lens = {}
  for i = 1, ncols do max_lens[i] = 3 end

  local function scan_rows(rows)
    for _, row in ipairs(rows) do
      local cells = row.cells or row
      for i, cell in ipairs(cells) do
        if i <= ncols then
          local len = cell_text_length(cell)
          if len > max_lens[i] then max_lens[i] = len end
        end
      end
    end
  end

  if tbl.head and tbl.head.rows then scan_rows(tbl.head.rows) end
  for _, body in ipairs(tbl.bodies or {}) do
    if body.body then scan_rows(body.body) end
  end

  local total = 0
  for i = 1, ncols do total = total + max_lens[i] end

  if total > 0 then
    local floor_total = MIN_COL_WIDTH * ncols
    local remaining = math.max(1.0 - floor_total, 0)
    for i = 1, ncols do
      tbl.colspecs[i][2] = MIN_COL_WIDTH + remaining * (max_lens[i] / total)
    end
  end

  if tbl.head and tbl.head.rows then process_rows(tbl.head.rows) end
  for _, body in ipairs(tbl.bodies or {}) do
    if body.body then process_rows(body.body) end
  end

  return tbl
end
