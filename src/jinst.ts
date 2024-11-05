import { EventEmitter } from 'events';
import java from 'java';

function isJvmCreated(): boolean {
  // return typeof java.onJvmCreated !== 'function';
  return java.isJvmCreated();
}

interface IJinst {
  isJvmCreated(): boolean;
  addOption(option: string): void;
  setupClasspath(dependencyArr: string[]): void;
  getJavaInstance(): typeof java;
  getStaticFieldValue(className: string, fieldName: string): any;
  callStaticMethod(className: string, methodName: string, ...args: any[]): Promise<any>;
  events: EventEmitter;
  import(className: string): any;
  newInstance(className: string, ...args: any[]): Promise<any>;
}

// const jinst: Jinst = {
//   isJvmCreated: function(): boolean {
//     return isJvmCreated();
//   },

//   addOption: function(option: string): void {
//     if (!isJvmCreated() && option) {
//       java.options.push(option);
//     } else if (isJvmCreated()) {
//       console.error("You've tried to add an option to an already running JVM!");
//       console.error("This isn't currently supported. Please add all option entries before calling any java methods");
//       console.error("You can test for a running JVM with the isJvmCreated function.");
//     }
//   },

//   setupClasspath: function(dependencyArr: string[]): void {
//     if (!isJvmCreated() && dependencyArr) {
//       java.classpath.push(...dependencyArr);
//     } else if (isJvmCreated()) {
//       console.error("You've tried to add an entry to the classpath of an already running JVM!");
//       console.error("This isn't currently supported. Please add all classpath entries before calling any java methods");
//       console.error("You can test for a running JVM with the isJvmCreated function.");
//     }
//   },

//   getInstance: function(): typeof java {
//     return java;
//   },

//   events: new EventEmitter(),
// };

class Jinst implements IJinst{
  private static instance: Jinst | null = null;

  private constructor() {}

  // Singleton access method
  public static getInstance(): Jinst {
    if (!Jinst.instance) {
      Jinst.instance = new Jinst();
    }
    return Jinst.instance;
  }

  // Method to check if the JVM is created
  public isJvmCreated(): boolean {
    return isJvmCreated();
  }

  // Method to add JVM options
  public addOption(option: string): void {
    if (!isJvmCreated() && option) {
      java.options.push(option);
    } else if (isJvmCreated()) {
      console.error("You've tried to add an option to an already running JVM!");
      console.error("This isn't currently supported. Please add all option entries before calling any java methods");
      console.error("You can test for a running JVM with the isJvmCreated function.");
    }
  }

  // Method to set up the JVM classpath
  public setupClasspath(dependencyArr: string[]): void {
    if (!isJvmCreated() && dependencyArr) {
      java.classpath.push(...dependencyArr);
    } else if (isJvmCreated()) {
      console.error("You've tried to add an entry to the classpath of an already running JVM!");
      console.error("This isn't currently supported. Please add all classpath entries before calling any java methods");
      console.error("You can test for a running JVM with the isJvmCreated function.");
    }
  }

  // Method to access the JVM instance
  public getJavaInstance(): typeof java {
    return java;
  }
  public getStaticFieldValue(className: string, fieldName: string): any {
    return java.getStaticFieldValue(className, fieldName);
  }

  public callStaticMethod(className: string, methodName: string, ...args: any[]): any {
      return java.callStaticMethod.apply(java, [className, methodName, ...args])
  }

  public callStaticMethodSync(className: string, methodName: string, ...args: any[]): any {
    return java.callStaticMethod.apply(java, [className, methodName, ...args])
}
public import(className: string): any {
  return java.import(className); // Assuming `java.import` exists in the library
}

public newInstance(className: string, ...args: any[]): Promise<any> {
  return new Promise((resolve, reject) => {
    resolve(java.newInstance(className, ...args));
  });
}

  // Event emitter
  public events = new EventEmitter();
}
export default Jinst;
