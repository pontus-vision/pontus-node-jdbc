import { EventEmitter } from 'events';
import java from 'java';
import winston from 'winston';

function isJvmCreated(): boolean {
  return typeof java.onJvmCreated !== 'function';
}

interface Jinst {
  isJvmCreated(): boolean;
  addOption(option: string): void;
  setupClasspath(dependencyArr: string[]): void;
  getInstance(): typeof java;
  events: EventEmitter;
}

const jinst: Jinst = {
  isJvmCreated: function(): boolean {
    return isJvmCreated();
  },

  addOption: function(option: string): void {
    if (!isJvmCreated() && option) {
      java.options.push(option);
    } else if (isJvmCreated()) {
      winston.error("You've tried to add an option to an already running JVM!");
      winston.error("This isn't currently supported. Please add all option entries before calling any java methods");
      winston.error("You can test for a running JVM with the isJvmCreated function.");
    }
  },

  setupClasspath: function(dependencyArr: string[]): void {
    if (!isJvmCreated() && dependencyArr) {
      java.classpath.push(...dependencyArr);
    } else if (isJvmCreated()) {
      winston.error("You've tried to add an entry to the classpath of an already running JVM!");
      winston.error("This isn't currently supported. Please add all classpath entries before calling any java methods");
      winston.error("You can test for a running JVM with the isJvmCreated function.");
    }
  },

  getInstance: function(): typeof java {
    return java;
  },

  events: new EventEmitter(),
};

export default jinst;
