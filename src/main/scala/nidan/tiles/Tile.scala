package nidan.tiles

/**
 * @author debarron
 * 
 * With this class we will encapsulate the
 * tile concept.
 * @param img will be stored as a byte array
 * @param metadata It includes the image format as well as other information
 */
class Tile(var img:Array[Byte], var metadata:TileMetadata)
