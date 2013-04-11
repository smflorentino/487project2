package tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import javax.swing.JOptionPane;

public class FileDuplicator {
	public static void main(String[] args) throws IOException {
		String path = JOptionPane.showInputDialog("Input Folder Path:");
		File folder = new File(path);
		File[] list = folder.listFiles();
		
		for(int i=0;i<list.length;i++) {
			String fileName=list[i].getName();
			
			for(int j=0;j<2;j++) { //make two copies
				Files.copy(list[i].toPath(), FileSystems.getDefault().getPath(list[i].getName() + j + ""), StandardCopyOption.REPLACE_EXISTING);
			}
			
		}
		
		
	}
}
