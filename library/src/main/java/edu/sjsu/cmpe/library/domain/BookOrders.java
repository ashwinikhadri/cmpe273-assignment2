/**
 * 
 */
package edu.sjsu.cmpe.library.domain;

import com.yammer.dropwizard.jersey.params.LongParam;

/**
 * @author Ashwini
 *
 */
public class BookOrders {
	private String libraryName;
	private LongParam isbn;

	public String getLibraryName() {
		return libraryName;
	}
	public void setLibraryName(String libraryName) {
		this.libraryName = libraryName;
	}
	public LongParam getIsbn() {
		return isbn;
	}
	public void setIsbn(LongParam isbn) {
		this.isbn = isbn;
	}
}
