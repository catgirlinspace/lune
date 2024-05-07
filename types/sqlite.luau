local SQLiteDB = {}

function SQLiteDB:execute(sql: string, params: {any}?): number
    return nil :: any
end

function SQLiteDB:executeBatch(sql: string): void
    return nil :: any
end

export type QueryReturn<T = {[string]: any}> = {
    [number]: T
}

function SQLiteDB:query<T>(sql: string, params: {any}?): QueryReturn<T>
    return nil :: any
end

export type SQLiteDB = typeof(SQLiteDB)

local sqlite = {}

function sqlite.new(path: string): SQLiteDB
    return nil :: any
end

return sqlite
