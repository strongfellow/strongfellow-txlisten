package com.strongfellow.txlisten;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import org.bitcoinj.store.BlockStoreException;

import com.strongfellow.utils.ParseException;

public class Main {

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, ParseException, InterruptedException, ExecutionException, BlockStoreException {
		if (args[0].equals("read")) {
			BlockReader.main(args);
		} else if (args[0].equals("listen")) {
			TransactionListener.main(args);
		} else {
			System.exit(1);
		}
	}
}
