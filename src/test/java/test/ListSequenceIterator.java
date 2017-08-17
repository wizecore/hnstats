package test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;

import lombok.NonNull;

public class ListSequenceIterator implements SentenceIterator, Iterable<String> {

    private SentencePreProcessor preProcessor;
	private List<String> source;
	private Iterator<String> iter;
    
    public ListSequenceIterator(@NonNull List<String> l) throws IOException {
    	source = l;
    	iter = l.iterator();
    }
    
    @Override
    public synchronized String nextSentence() {
        try {
            String next = iter.next();
			return (preProcessor != null) ? this.preProcessor.preProcess(next) : next;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public synchronized void reset() {
    	iter = source.iterator();
    }

    @Override
    public void finish() {
    }

    @Override
    public SentencePreProcessor getPreProcessor() {
        return preProcessor;
    }

    @Override
    public void setPreProcessor(SentencePreProcessor preProcessor) {
        this.preProcessor = preProcessor;
    }

    @Override
    protected void finalize() throws Throwable {
    }

    /**
     * Implentation for Iterable interface.
     * Please note: each call for iterator() resets underlying SentenceIterator to the beginning;
     *
     * @return
     */
    @Override
    public Iterator<String> iterator() {
        this.reset();
        Iterator<String> ret = new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return ListSequenceIterator.this.hasNext();
            }

            @Override
            public String next() {
                return ListSequenceIterator.this.nextSentence();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return ret;
    }
}