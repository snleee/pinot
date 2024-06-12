package org.apache.pinot.plugin.minion.tasks.industryv2;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


public class IndustryV2RecordReader implements RecordReader {

  private PinotSegmentRecordReader _pinotSegmentRecordReader;
  private String _industryV1Column;
  private String _industryV2Column;

  public IndustryV2RecordReader(String industryV1Column, String industryV2Column) {
    _industryV1Column = industryV1Column;
    _industryV2Column = industryV2Column;
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _pinotSegmentRecordReader = new PinotSegmentRecordReader();
    _pinotSegmentRecordReader.init(dataFile, fieldsToRead, recordReaderConfig);
  }

  @Override
  public boolean hasNext() {
    return _pinotSegmentRecordReader.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    return transform(_pinotSegmentRecordReader.next(reuse));
  }

  @Override
  public void rewind()
      throws IOException {
    _pinotSegmentRecordReader.rewind();
  }

  @Override
  public void close()
      throws IOException {
    _pinotSegmentRecordReader.close();
  }

  private GenericRow transform(GenericRow row) {
    Integer v2ColumnValue = (Integer) row.getValue(_industryV2Column);
    // Industry V2 migration logic.
    if (v2ColumnValue == null || v2ColumnValue == Integer.MIN_VALUE) {
      Integer v1ColumnValue = (Integer) row.getValue(_industryV1Column);
      row.putValue(_industryV2Column, v1ColumnValue);
    }
    return row;
  }
}
