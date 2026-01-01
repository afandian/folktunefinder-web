const path = require("path");

const HtmlWebpackPlugin = require("html-webpack-plugin");
const { CleanPlugin } = require("webpack");

module.exports = [
  {
    entry: {
      index: "./src/index.ts",
    },

    module: {
      rules: [
        {
          test: /\.tsx?$/,
          use: "ts-loader",
          exclude: /node_modules/,
        },
      ],
    },
    resolve: {
      extensions: [".tsx", ".ts", ".js"],
    },

    plugins: [
      new HtmlWebpackPlugin({
        title: "Output Management",
        template: "./src/index.html",
      }),
    ],
    output: {
      filename: "[name].bundle.js",
      path: path.resolve(__dirname, "dist"),
      clean: true,
    },
  },

  // {
  //   entry: {
  //     // index: "./src/frontend/index.js",
  //     index: "./src/backend/indexTS.ts",
  //     //common: "./src/shared/common.js",
  //   },

  //   module: {
  //     rules: [
  //       {
  //         test: /\.tsx?$/,
  //         use: "ts-loader",
  //         exclude: /node_modules/,
  //       },
  //     ],
  //   },
  //   resolve: {
  //     extensions: [".tsx", ".ts", ".js"],
  //   },

  //   output: {
  //     filename: "[name].backend-bundle.js",
  //     path: path.resolve(__dirname, "dist-backend"),
  //     clean: true,
  //   },
  // },
];
