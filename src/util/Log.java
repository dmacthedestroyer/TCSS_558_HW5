package util;
/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing
 * Institute of Technology, UW Tacoma
 * Written by Daniel M. Zimmerman
 */

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * A very simple logging class that prints to either standard error or
 * standard output, while adding timestamps to the messages.
 * 
 * @author Daniel M. Zimmerman
 * @version Spring 2013
 */
public final class Log
{
  /**
   * The date formatter to be used.
   */
  private static final SimpleDateFormat DATE = 
    new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS", Locale.US);
  
  /**
   * The separator between the date and the message.
   */
  private static final String SEPARATOR = ": ";
  
  /**
   * A constructor to prevent instantiation.
   */
  private Log()
  {
    // do nothing
  }
  
  /**
   * Print a log message to standard output.
   * 
   * @param the_message The log message.
   */
  public static synchronized void out(final String the_message)
  {
    System.out.println(DATE.format(new Date()) + SEPARATOR + the_message);
  }
  
  /**
   * Pring a formatted log message to standard output.
   * @param the_message The log message
   * @param args Arguments referenced by the format specifiers in the format string. If there are more arguments than format specifiers, the extra arguments are ignored. The number of arguments is variable and may be zero. The maximum number of arguments is limited by the maximum dimension of a Java array as defined by The Java™ Virtual Machine Specification. The behaviour on a null argument depends on the conversion.
   */
  public static synchronized void out(final String the_message, final Object... args){
  	System.out.println(DATE.format(new Date()) + SEPARATOR + String.format(the_message,  args));
  }
  
  /**
   * Print a log message to standard error.
   * 
   * @param the_message The log message.
   */
  public static synchronized void err(final String the_message)
  {
    System.err.println(DATE.format(new Date()) + SEPARATOR + the_message);
  }
  
  public static synchronized void err(final String the_message, final Object... args){
  	System.err.println(DATE.format(new Date()) + SEPARATOR + String.format(the_message,  args));
  }
}
