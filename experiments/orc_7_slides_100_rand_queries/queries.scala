val queries = List(
("SELECT imageBytes from data where partitionZIndex>=1 and partitionZIndex<=1",1),
("SELECT imageBytes from data where partitionZIndex>=1 and partitionZIndex<=2",2),
("SELECT imageBytes from data where partitionZIndex>=1 and partitionZIndex<=4",4),
("SELECT imageBytes from data where partitionZIndex>=1 and partitionZIndex<=8",8)
)

