package edu.rutgers.ess.crs.utility;

import java.io.IOException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class CSVFileUtil
{
    public static boolean mergeToLocal(final FileSystem fs, final Path src, final FileSystem localFS, final Path dst, final Configuration conf, final boolean deleteSrc, final boolean deleteDst) {
        try {
            if (deleteDst && localFS.exists(dst)) {
                localFS.delete(dst, true);
            }
            FileUtil.copyMerge(fs, src, localFS, dst, deleteSrc, conf, "");
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
