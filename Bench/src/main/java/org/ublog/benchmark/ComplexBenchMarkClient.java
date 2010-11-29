/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vilaça and Francisco Cruz
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

public abstract class ComplexBenchMarkClient implements BenchMarkClient {

	private int clientID;
	private BenchOperation currentComplexOperation;

	private int peerID;

	public ComplexBenchMarkClient(int clientID) {
		this.clientID = clientID;
		this.currentComplexOperation = null;
	}

	@Override
	public boolean hasMoreOperations() {
		return this.hasMoreComplexOperations()
				|| (this.currentComplexOperation != null && this.currentComplexOperation
						.hasMoreDBOperations());
	}

	public abstract boolean hasMoreComplexOperations();

	public abstract Pair<BenchOperation, Double> getNextComplexOperation();

	public abstract void handleFinishedComplexOperation(BenchOperation op);

	@Override
	public synchronized Pair<CollectionOperation, Double> nextOperation()
			throws UnsupportedEncodingException {
		CollectionOperation res;
		double time = 0.0;
		if (this.currentComplexOperation == null) {
			Pair<BenchOperation, Double> next = this.getNextComplexOperation();
			this.currentComplexOperation = next.getFirst();
			time = next.getSecond();
		} else {
			if (!this.currentComplexOperation.hasMoreDBOperations()) {
				Pair<BenchOperation, Double> next = this
						.getNextComplexOperation();
				this.currentComplexOperation = next.getFirst();
				time = next.getSecond();
			}
		}
		res = this.currentComplexOperation.getNextDBOperation();
		return new Pair<CollectionOperation, Double>(res, time);
	}

	@Override
	public int getClientID() {
		return this.clientID;
	}

	public void setPeerID(int peerID) {
		this.peerID = peerID;
	}

	public int getPeerID() {
		return peerID;
	}

}
