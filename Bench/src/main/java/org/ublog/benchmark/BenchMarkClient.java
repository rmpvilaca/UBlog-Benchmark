/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vila�a and Francisco Cruz
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ublog.benchmark;

import java.io.UnsupportedEncodingException;

import org.ublog.utils.Pair;
import org.ublog.benchmark.operations.CollectionOperation;

public interface BenchMarkClient {
	public boolean hasMoreOperations();

	public Pair<CollectionOperation, Double> nextOperation()
			throws UnsupportedEncodingException;

	public int getClientID();

	public void setPeerID(int peerID);

	public int getPeerID();

}
