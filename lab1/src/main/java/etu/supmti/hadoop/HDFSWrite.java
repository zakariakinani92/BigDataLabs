package etu.supmti.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: HDFSWrite <nomcomplet> <text>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);

        // Supprimer le fichier existant s'il existe
        if (fs.exists(nomcomplet)) {
            fs.delete(nomcomplet, true);
        }

        // Créer et écrire dans le fichier
        try (FSDataOutputStream outStream = fs.create(nomcomplet)) {
            outStream.writeBytes("Bonjour tout le monde !\n");
            outStream.writeBytes(args[1] + "\n");
        }

        fs.close();
    }
}
