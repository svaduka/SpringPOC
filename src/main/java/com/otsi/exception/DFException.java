package com.otsi.exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DFException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static String exceptionMsg;
	private static List<String> softMsgs=new ArrayList<String>();
	private static List<String> errorMsgs=new ArrayList<String>();

	public DFException(Exception e){
		System.out.println("DFException");
		e.printStackTrace();
		//TODO process Exception Messages
		if(e instanceof IOException)
		{
			//TODO do IOException related stuff
		}
	}
	/**
	 * 
	 * @param exceptionMsg
	 * @param isSoftMsg
	 * Adding Exception Messages
	 */
	public DFException(final String exceptionMsg, boolean isSoftMsg){
		this.exceptionMsg=exceptionMsg;
		if(isSoftMsg){
			softMsgs.add(exceptionMsg);
		}else{
			errorMsgs.add(exceptionMsg);
		}
	}
	/**
	 * 
	 * @param exceptionMsg
	 * Creating Error Messages
	 */
	public DFException(final String exceptionMsg){
		new DFException(exceptionMsg, Boolean.FALSE);
	}
	/**
	 * 
	 * @return TRUE/FALSE to indicate whether any warnings or messages exists
	 */
	public static boolean haveMsgs(){
		if(!softMsgs.isEmpty() || !errorMsgs.isEmpty())
		{
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
