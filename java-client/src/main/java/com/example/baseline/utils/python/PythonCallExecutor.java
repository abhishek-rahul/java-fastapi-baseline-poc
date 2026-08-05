package com.example.baseline.utils.python;

interface PythonCallExecutor extends AutoCloseable {
    PythonCallResponse call(PythonCallRequest request) throws Exception;

    @Override
    void close();
}
