package gui.util;

import java.awt.event.ActionListener;

import javax.swing.JButton;

/**
 * Simple JButton class that takes in as parameters the only thing I care about
 * for creating buttons: the text on the button and what to do when you click
 * it.
 * 
 * @author dmac
 * 
 */
public class JAutoSubscribeActionButton extends JButton {
	/**
	 * creates a button with the given text and sets the action to the given
	 * ActionListener
	 * 
	 * @param buttonText
	 * @param actionListener
	 */
	public JAutoSubscribeActionButton(String buttonText, ActionListener actionListener) {
		super(buttonText);
		addActionListener(actionListener);
	}
}
