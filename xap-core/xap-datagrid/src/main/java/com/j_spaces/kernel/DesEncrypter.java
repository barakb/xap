/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.kernel;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

/**
 * Encrypting a String with DES private key algorithm. This class implements an encrypting and
 * decrypting strings using DES. The class is created with a key and can be used repeatedly to
 * encrypt and decrypt strings using that key.
 *
 * @author Michael Konnikov
 * @version 4.1
 */

@com.gigaspaces.api.InternalApi
public class DesEncrypter {
    Cipher ecipher;
    Cipher dcipher;

    //logger
    final private static Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_KERNEL);

    public static DesEncrypter getInstance() {
        try {
            SecretKeyFactory keyFac = SecretKeyFactory.getInstance("DES");
            SecretKey desKey = keyFac.generateSecret(new DESKeySpec("gigaspaces-security".getBytes("UTF-8")));
            return new DesEncrypter(desKey);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex.getCause());
        }
    }


    public DesEncrypter(Key key) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidKeyException {
        ecipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        dcipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        ecipher.init(Cipher.ENCRYPT_MODE, key);
        dcipher.init(Cipher.DECRYPT_MODE, key);
        //System.out.println("Cipher " + ecipher.getAlgorithm() + ", " + ecipher.getBlockSize());
        //System.out.println("Provider " + ecipher.getProvider().getName() + ", " + ecipher.getProvider().getInfo());
    }

    public String encrypt(String str) {
        try {
            // Encode the string into bytes using utf-8
            byte[] utf8 = str.getBytes("UTF-8");

            // Encrypt
            byte[] enc = ecipher.doFinal(utf8);

            // Encode bytes to base64 to get a string
            return new sun.misc.BASE64Encoder().encode(enc);
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
        return null;
    }

    public String decrypt(String str) {
        try {
            // Decode base64 to get bytes
            byte[] dec = new sun.misc.BASE64Decoder().decodeBuffer(str);

            // Decrypt
            byte[] utf8 = dcipher.doFinal(dec);

            // Decode using utf-8
            return new String(utf8, "UTF-8");
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
        return null;
    }


    public byte[] encrypt(byte[] byteArray) {
        try {
            return ecipher.doFinal(byteArray);
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
        return null;
    }

    public byte[] dencrypt(byte[] byteArray) {
        try {
            return dcipher.doFinal(byteArray);
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
        return null;
    }

}