package com.google.cloud.training.dataanalyst.javahelp;

import java.io.Serializable;

import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

//@SuppressWarnings("serial")
//class GroupStrings extends CombineFn<String, GroupStrings.Accum, List<KV<String, List<String>>>> {
//	
//	List<String> searchTerms;
//	
////	GroupStrings(){
////		this.searchTerms = searchTerms;
////	}
//	
//	public static class Accum implements Serializable{
//		KV<String, List<String>> ini = KV.of("Ini", new ArrayList<String>());
//		KV<String, List<String>> arya = KV.of("Arya", new ArrayList<String>());
////		KV<String, String> label;
//	}
//	
//	@Override
//	public Accum createAccumulator() { return new Accum(); }
//	
//	@Override 
//	public Accum addInput(Accum accum, String input) {
//		
////		if (input.contains("Ini")) {
////			
////			List<String> temp = accum.ini.getValue();
////			temp.add(input);
////			accum.label = KV.of("Ini", accum.label);
////		
////		}else if(input.contains("Arya")) {
////			
////			List<String> temp = accum.arya.getValue();
////			temp.add(input);
////			accum.arya = KV.of("Arya", temp);
////			
////		}
//		
//		return accum;
//	}
//	
//	@Override
//	public Accum mergeAccumulators(Iterable<Accum> accums) {
//		Accum merged = createAccumulator();
//		
//		for(Accum accum: accums) {
//			
//			List<String> tempini = accum.ini.getValue();
//			List<String> tempmerge = merged.ini.getValue();
//			
//			tempmerge.addAll(tempini);
//			
//			List<String> temparya = accum.arya.getValue();
//			List<String> tempmergearya = merged.arya.getValue();
//			
//			tempmergearya.addAll(temparya);
//			
//		}
//		
//		return merged;
//	}
//	
//	@Override
//	public List<KV<String, List<String>>> extractOutput(Accum accum) {
//		
//		return Arrays.asList(accum.ini, accum.arya);
//	}
//}

@SuppressWarnings("serial")
class PTransformWriteToGCS extends PTransform<PCollection<KV<String, Iterable<String>>>, PCollection<Void>> implements Serializable{
	
	private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
	
	private final String bucketName;

	public PTransformWriteToGCS(final String bucketName) {

	this.bucketName = bucketName;
	
	}
	
	@Override
	public PCollection<Void> expand(PCollection<KV<String, Iterable<String>>> input) {
	
	return input
	        .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
	
	            @ProcessElement
	            public void processElement(
	                    final DoFn<KV<String, Iterable<String>>, Void>.ProcessContext c)
	                    throws Exception {
	                final String key = c.element().getKey();
	                
	                String toWrite = "";
	    			for(String line : c.element().getValue()) {
	    				toWrite += line + "\n";
	    			}

	                String filename = "javahelp/output/" + key + ".txt";
	                BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, filename)
	                        .setContentType(MimeTypes.TEXT)
	                        .build();

	                STORAGE.create(blobInfo, toWrite.getBytes(StandardCharsets.UTF_8));
	            }
	        }));
	}
}

public class GreptoMultipleFiles {

	@SuppressWarnings("serial")
	static class stoi extends DoFn<String, Integer>{
		@ProcessElement
		public void processElement(@Element String word, OutputReceiver<Integer> out) {
			String[] elements = word.split(",");
			out.output(Integer.parseInt(elements[1].trim()));
		}
	}
	
	@SuppressWarnings("serial")
	static class itos extends DoFn<Double, String>{
		@ProcessElement
		public void processElement(@Element Integer word, OutputReceiver<String> out) {
			out.output(Integer.toString(word));
		}
	}
	
	@SuppressWarnings("serial")
	static class ExtractWordsFn extends DoFn<String, KV<String, String>> {
		
		@ProcessElement
		public void processElement(@Element String line, OutputReceiver<KV<String, String>> out) {
			
			
			if(line.contains("Ini")) {
				out.output(KV.of("Ini", line));
			}
			else if(line.contains("Arya")) {
				out.output(KV.of("Arya", line));
			}
			else if(line.contains("Sreekar")) {
				out.output(KV.of("Sreekar", line));
			}
			else {
				out.output(KV.of("Unknown", line));
			}
		}
	}
	
	@SuppressWarnings("serial")
	static class PrintGroupBy extends DoFn<KV<String, Iterable<String>>, String> {
		
		@ProcessElement
		public void processElement(ProcessContext c, OutputReceiver<String> out) {
			String key = ">>>>>" + c.element().getKey() + "<<<<< :\n";
			String value = "";
			for(String line : c.element().getValue()) {
				value += line + "\n";
			}
			
			String result = key + value;
			out.output(result);
		}
	}
	
	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		Pipeline pipeline = Pipeline.create(options);

		String input = "gs://dataflow-results-ini/javahelp/input.txt";
//		String outputPrefix = "gs://dataflow-results-ini/javahelp/output";

		PCollection<String> readFile= pipeline.apply("GetJava", TextIO.read().from(input));

		PCollection<KV<String, String>> labeled = readFile.apply(ParDo.of(new ExtractWordsFn()));
		
		PCollection<KV<String, Iterable<String>>> together = labeled.apply(GroupByKey.create());
		
//		PCollection<String> result= together.apply(ParDo.of(new PrintGroupBy()));
		
//		PCollection<List<KV<String, List<String>>>> groupedSearch = readFile.apply(Combine.globally(new GroupStrings()).withoutDefaults());
		
//		result.apply(TextIO.write().to(outputPrefix).withSuffix(".txt").withoutSharding());

		together.apply(new PTransformWriteToGCS("dataflow-results-ini"));
		
		pipeline.run();
	}
}


