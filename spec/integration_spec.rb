require './lib/multi_db'

describe MultiDb do

  it 'sends queries to the master by default'

  it 'sends queries to the slave inside a with_slave block'

  it 'sends all queries to the master inside a transaction'

  it 'rolls back the connection stack even if a transaction exploded'

  it 'stickies connections to the master by session'

end
