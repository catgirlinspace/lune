local SQLiteDB = {}

function SQLiteDB:execute(sql: string, params: {any}?): number
    return nil :: any
end

type QueryReturn = {
    [number]: { [string]: any }
}

function SQLiteDB:query(sql: string, params: {any}?): QueryReturn
    return nil :: any
end

export type SQLiteDB = typeof(SQLiteDB)

local sqlite = {}

function sqlite.new(path: string): SQLiteDB
    return nil :: any
end
