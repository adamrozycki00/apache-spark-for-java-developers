package com.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GlobalSettings {

    public static void setHadoopAndLogger() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.ERROR);
    }

}
