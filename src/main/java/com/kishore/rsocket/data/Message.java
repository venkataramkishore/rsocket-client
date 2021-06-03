/**
 * 
 */
package com.kishore.rsocket.data;

import java.time.Instant;

/**
 * @author kishore
 *
 */
public class Message {

	private String message;
	private long created = Instant.now().getEpochSecond();
	

	public Message() {
		// TODO Auto-generated constructor stub
	}
	public Message(String message) {
		this.message = message;
	}


	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}


	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}


	/**
	 * @return the created
	 */
	public long getCreated() {
		return created;
	}


	/**
	 * @param created the created to set
	 */
	public void setCreated(long created) {
		this.created = created;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Message [message=");
		builder.append(message);
		builder.append(", created=");
		builder.append(created);
		builder.append("]");
		return builder.toString();
	} 
	
}
