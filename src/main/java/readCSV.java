import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class readCSV {
    public static void main(String[] args) {
        parseFile(new File("src/main/resources/Friends.txt"));

    }
    public static List<String[]> parseFile(File file){
        List<String[]> toReturn = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(file))){
            for (String[] a : reader){

            }



        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Just commenting
        return toReturn;
    }
}
