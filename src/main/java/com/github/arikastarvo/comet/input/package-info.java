/**
 * Generic input related resources (specific input modules are in sub-packages)
 * 
 * <h3>abstract classes</h3>
 * <p>
 *	{@link com.github.arikastarvo.comet.input.Input} - All inputs must extend this abstract class<br/>
 *	{@link com.github.arikastarvo.comet.input.InputConfiguration} - All inputs  are initialized by a configuration class that must extend this class
 * </p>
 * 
 * <h3>interfaces</h3>
 * <p>
 *	{@link com.github.arikastarvo.comet.input.FiniteInput} marks inputs that have the capability to finish and shut down (ex: close input after reading file)<br/>
 *	{@link com.github.arikastarvo.comet.input.RepeatableInput} marks {@link com.github.arikastarvo.comet.input.FiniteInput} classes that have the capability to repeatedly re-read data (making them non-finite again)<br/>
 *	{@link com.github.arikastarvo.comet.input.ReferenceInput} - undocumented and a bit wobbly solution for now<br/>
 *	<br/>
 *	{@link com.github.arikastarvo.comet.input.URICapableInputConfiguration} - for input {@link com.github.arikastarvo.comet.input.InputConfiguration} classes that support URI style input definitions
 * </p>
 * 
 * <h3>exceptions</h3s>
 * <p>
 * 	{@link com.github.arikastarvo.comet.input.InputDefinitionException}
 * </p>
 */
package com.github.arikastarvo.comet.input;