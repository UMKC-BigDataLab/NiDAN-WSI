import java.io.File
import java.io.FileOutputStream
import org.apache.spark.sql._

val queryMsg = "#QUERY "
val loadDBMsg = "#LOAD_DB "
val loadTable = "#LOAD_TABLE "
val loadsqlHive = "#LOAD_SQL_CONTEXT "
val dataSource = "/nidan/orc/individualORC/slide1"

def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
    1
  }
  
  val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 49  OR  partitionIndex = 50 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 60  OR  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 83  OR  partitionIndex = 84  OR  partitionIndex = 85 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 57  OR  partitionIndex = 58  OR  partitionIndex = 59  OR  partitionIndex = 60 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 120  OR  partitionIndex = 121  OR  partitionIndex = 122  OR  partitionIndex = 123  OR  partitionIndex = 135  OR  partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 138 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 147  OR  partitionIndex = 148  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 175  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 156  OR  partitionIndex = 157  OR  partitionIndex = 169  OR  partitionIndex = 170 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 227  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 87  OR  partitionIndex = 88  OR  partitionIndex = 89  OR  partitionIndex = 116 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 145  OR  partitionIndex = 146  OR  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 173  OR  partitionIndex = 174 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 19  OR  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 48 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 233  OR  partitionIndex = 234 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 65  OR  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 79  OR  partitionIndex = 80  OR  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 94 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 120  OR  partitionIndex = 120  OR  partitionIndex = 121  OR  partitionIndex = 122  OR  partitionIndex = 123  OR  partitionIndex = 135  OR  partitionIndex = 136  OR  partitionIndex = 137 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 27  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 30 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 85  OR  partitionIndex = 86  OR  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 113  OR  partitionIndex = 114 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 147  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 179  OR  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 203  OR  partitionIndex = 204 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 89  OR  partitionIndex = 90  OR  partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 117  OR  partitionIndex = 118 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 62  OR  partitionIndex = 63  OR  partitionIndex = 76  OR  partitionIndex = 77  OR  partitionIndex = 78  OR  partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 105 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 124  OR  partitionIndex = 151  OR  partitionIndex = 152  OR  partitionIndex = 153  OR  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 168 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 104  OR  partitionIndex = 105  OR  partitionIndex = 119  OR  partitionIndex = 120  OR  partitionIndex = 120  OR  partitionIndex = 121  OR  partitionIndex = 135  OR  partitionIndex = 136 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 19  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 22 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 53  OR  partitionIndex = 54 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 72  OR  partitionIndex = 99  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 113  OR  partitionIndex = 114  OR  partitionIndex = 115  OR  partitionIndex = 116 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 179  OR  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 203  OR  partitionIndex = 204 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 233  OR  partitionIndex = 234 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 154  OR  partitionIndex = 155 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 138  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 152  OR  partitionIndex = 153  OR  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 167 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 145  OR  partitionIndex = 146  OR  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 173  OR  partitionIndex = 174 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 201  OR  partitionIndex = 202  OR  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 229  OR  partitionIndex = 230 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 2  OR  partitionIndex = 3  OR  partitionIndex = 16  OR  partitionIndex = 17  OR  partitionIndex = 18  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 45 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 109  OR  partitionIndex = 110 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 143  OR  partitionIndex = 144  OR  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 231  OR  partitionIndex = 232 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 130  OR  partitionIndex = 143  OR  partitionIndex = 144  OR  partitionIndex = 217  OR  partitionIndex = 231  OR  partitionIndex = 232 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 64  OR  partitionIndex = 91  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 105  OR  partitionIndex = 106  OR  partitionIndex = 107  OR  partitionIndex = 108 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 49  OR  partitionIndex = 50  OR  partitionIndex = 51  OR  partitionIndex = 52 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 122  OR  partitionIndex = 123  OR  partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 165 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 109  OR  partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 112 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 109  OR  partitionIndex = 110 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 7  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 36  OR  partitionIndex = 49  OR  partitionIndex = 50 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 211  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 2  OR  partitionIndex = 3  OR  partitionIndex = 17  OR  partitionIndex = 18  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 45  OR  partitionIndex = 46 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 121  OR  partitionIndex = 122  OR  partitionIndex = 123  OR  partitionIndex = 135  OR  partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 150 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 206  OR  partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 233  OR  partitionIndex = 234  OR  partitionIndex = 235 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 186  OR  partitionIndex = 187  OR  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 201  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 124  OR  partitionIndex = 125  OR  partitionIndex = 126  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 153  OR  partitionIndex = 167  OR  partitionIndex = 168 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 9  OR  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 23  OR  partitionIndex = 24  OR  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 38 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 23  OR  partitionIndex = 24  OR  partitionIndex = 111  OR  partitionIndex = 112 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 77  OR  partitionIndex = 78  OR  partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 105  OR  partitionIndex = 106 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 147  OR  partitionIndex = 148  OR  partitionIndex = 161  OR  partitionIndex = 175  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 45  OR  partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 48 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 124  OR  partitionIndex = 125  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 167  OR  partitionIndex = 168 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 109  OR  partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 112 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 69  OR  partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 83  OR  partitionIndex = 84  OR  partitionIndex = 85  OR  partitionIndex = 86  OR  partitionIndex = 98 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 143  OR  partitionIndex = 144  OR  partitionIndex = 231  OR  partitionIndex = 232 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 207  OR  partitionIndex = 208  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 235  OR  partitionIndex = 236 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 159  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 186  OR  partitionIndex = 187  OR  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 201  OR  partitionIndex = 202 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 207  OR  partitionIndex = 208  OR  partitionIndex = 221  OR  partitionIndex = 235  OR  partitionIndex = 236 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 19  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 48 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 229  OR  partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 232 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 119  OR  partitionIndex = 120  OR  partitionIndex = 120  OR  partitionIndex = 121  OR  partitionIndex = 122  OR  partitionIndex = 123  OR  partitionIndex = 135  OR  partitionIndex = 136 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 199  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 228 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 19  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34  OR  partitionIndex = 35 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 185  OR  partitionIndex = 186  OR  partitionIndex = 187  OR  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 201  OR  partitionIndex = 202  OR  partitionIndex = 214 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 165  OR  partitionIndex = 179  OR  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190  OR  partitionIndex = 203  OR  partitionIndex = 204 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 149  OR  partitionIndex = 150  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 177  OR  partitionIndex = 178 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 124  OR  partitionIndex = 125  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 168 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 105  OR  partitionIndex = 106  OR  partitionIndex = 107  OR  partitionIndex = 108 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 113  OR  partitionIndex = 114  OR  partitionIndex = 115  OR  partitionIndex = 116 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 64  OR  partitionIndex = 65  OR  partitionIndex = 79  OR  partitionIndex = 80  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 107  OR  partitionIndex = 108 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 15  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 44  OR  partitionIndex = 57  OR  partitionIndex = 58 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 123  OR  partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 152  OR  partitionIndex = 165  OR  partitionIndex = 166 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 105  OR  partitionIndex = 119  OR  partitionIndex = 120  OR  partitionIndex = 120  OR  partitionIndex = 121  OR  partitionIndex = 122  OR  partitionIndex = 135  OR  partitionIndex = 136 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 207  OR  partitionIndex = 208  OR  partitionIndex = 209  OR  partitionIndex = 236 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 58  OR  partitionIndex = 59  OR  partitionIndex = 60  OR  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 83 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 57  OR  partitionIndex = 58 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 87  OR  partitionIndex = 88  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 115  OR  partitionIndex = 116 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 203  OR  partitionIndex = 204  OR  partitionIndex = 205 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 124  OR  partitionIndex = 125  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 152  OR  partitionIndex = 153  OR  partitionIndex = 167  OR  partitionIndex = 168 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 35  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 49  OR  partitionIndex = 50  OR  partitionIndex = 51  OR  partitionIndex = 52  OR  partitionIndex = 60 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 53  OR  partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 56 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 27  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 42 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 125  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 139  OR  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 154 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 87  OR  partitionIndex = 88  OR  partitionIndex = 89  OR  partitionIndex = 90 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 80  OR  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 109 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 27  OR  partitionIndex = 28  OR  partitionIndex = 41  OR  partitionIndex = 55  OR  partitionIndex = 56 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 179  OR  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 198  OR  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 27  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 56 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 27  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 42  OR  partitionIndex = 43 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 59  OR  partitionIndex = 60  OR  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 83  OR  partitionIndex = 84 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 10  OR  partitionIndex = 23  OR  partitionIndex = 24  OR  partitionIndex = 97  OR  partitionIndex = 111  OR  partitionIndex = 112 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 147  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 176 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 49  OR  partitionIndex = 50 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 64  OR  partitionIndex = 65  OR  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 79  OR  partitionIndex = 80  OR  partitionIndex = 81  OR  partitionIndex = 108 ", 8),
("SELECT imageBytes FROM data WHERE  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 169 ", 8)
)

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query._1}")
show_timing{sqlContext.sql(query._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}

