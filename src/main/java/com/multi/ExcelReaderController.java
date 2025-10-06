package com.multi;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/excel")
public class ExcelReaderController {

    private static final int THREAD_COUNT = 5;

    @PostMapping("/upload")
    public ResponseEntity<?> uploadExcel(@RequestParam("file") MultipartFile file) {
        AtomicInteger rowCounter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        try (InputStream is = file.getInputStream();
             OPCPackage pkg = OPCPackage.open(is)) {

            // Initialize Apache POI streaming reader
            XSSFReader reader = new XSSFReader(pkg);
            StylesTable styles = reader.getStylesTable();
            SharedStrings sharedStrings = reader.getSharedStringsTable(); 

            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            while (sheets.hasNext()) {
                try (InputStream sheetStream = sheets.next()) {
                    processSheet(styles, sharedStrings, sheetStream, executor, rowCounter);
                }
            }

            // Wait for all threads to finish
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }

        return ResponseEntity.ok("Total Rows Processed: " + rowCounter.get());
    }

    private void processSheet(StylesTable styles,
                              SharedStrings sharedStrings,
                              InputStream sheetInputStream,
                              ExecutorService executor,
                              AtomicInteger counter) throws Exception {

        XMLReader parser = XMLReaderFactory.createXMLReader();

        XSSFSheetXMLHandler handler = new XSSFSheetXMLHandler(
                styles,
                sharedStrings,
                new XSSFSheetXMLHandler.SheetContentsHandler() {



                    @Override
                    public void startRow(int rowNum) {
                    	executor.submit(() -> {
                            counter.incrementAndGet();
                        });
                    }

                    @Override
                    public void endRow(int rowNum) {
                        
                    }
                    @Override
                    public void cell(String cellReference, String formattedValue, XSSFComment comment) {

                    }

                    @Override
                    public void headerFooter(String text, boolean isHeader, String tagName) { }
                },
                false
        );

        parser.setContentHandler(handler);
        parser.parse(new InputSource(sheetInputStream));
    }


}
